// Copyright 2021 hardcore-os Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package lsm

import (
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"unsafe"

	"github.com/hardcore-os/corekv/file"
	"github.com/hardcore-os/corekv/iterator"
	"github.com/hardcore-os/corekv/utils"
	"github.com/hardcore-os/corekv/utils/codec"
	"github.com/hardcore-os/corekv/utils/codec/pb"
)

// TODO LAB 这里实现 序列化

type tableBuilder struct {
	curBlock   *block
	opt        *Options
	blockList  []*block
	keyCount   uint32
	keyHashes  []uint32
	maxVersion uint64
	baseKey    []byte
}

type buildData struct {
	blockList []*block
	index     []byte
	checksum  []byte
	size      int
}

type block struct {
	offset            int
	checksum          []byte
	entriesIndexStart int
	chkLen            int
	data              []byte
	baseKey           []byte
	entryOffsets      []uint32
	end               int
}

type header struct {
	overlap uint16
	diff    uint16
}

const headerSize = uint16(unsafe.Sizeof(header{}))

// Decode decodes the header
func (h *header) decode(buf []byte) {
	copy(((*[headerSize]byte)(unsafe.Pointer(h))[:]), buf[:headerSize])
}

func (h header) encode() []byte {
	var b [4]byte
	*(*header)(unsafe.Pointer(&b[0])) = h
	return b[:]
}

func (tb *tableBuilder) add(e *codec.Entry) {
	key := e.Key
	// 检查是否需要分配一个新的block
	if tb.tryFinishBlock(e) {
		tb.finishBlock()
		// Create a new block and start writing
		tb.curBlock = &block{
			data: make([]byte, tb.opt.BlockSize),
		}
	}

	tb.keyHashes = append(tb.keyHashes, utils.Hash(codec.ParseKey(key)))
	if version := codec.ParseTs(key); version > tb.maxVersion {
		tb.maxVersion = version
	}

	var diffKey []byte
	if len(tb.curBlock.baseKey) == 0 {
		tb.curBlock.baseKey = append(tb.curBlock.baseKey[:0], key...)
		diffKey = key
	} else {
		diffKey = tb.keyDiff(key)
	}
	utils.CondPanic(!(len(key)-len(diffKey) <= math.MaxUint16), fmt.Errorf("tableBuilder.add: len(key)-len(diffKey) <= math.MaxUint16"))
	utils.CondPanic(!(len(diffKey) <= math.MaxUint16), fmt.Errorf("tableBuilder.add: len(diffKey) <= math.MaxUint16"))

	h := header{
		overlap: uint16(len(key) - len(diffKey)),
		diff:    uint16(len(diffKey)),
	}

	tb.curBlock.entryOffsets = append(tb.curBlock.entryOffsets, uint32(tb.curBlock.end))

	tb.append((h.encode()))
	tb.append(diffKey)

	dst := tb.allocate(int(e.EncodedSize()))
	e.EncodeEntry(dst)
}

func newTableBuilder(opt *Options) *tableBuilder {
	return &tableBuilder{
		opt: opt,
	}
}

func (tb *tableBuilder) tryFinishBlock(e *codec.Entry) bool {
	if tb.curBlock == nil {
		return true
	}

	if len(tb.curBlock.entryOffsets) <= 0 {
		return false
	}

	utils.CondPanic(!((uint32(len(tb.curBlock.entryOffsets))+1)*4+4+8+4 < math.MaxUint32), errors.New("Integer overflow"))
	entriesOffsetsSize := uint32((len(tb.curBlock.entryOffsets)+1)*4 +
		4 + // size of list
		8 + // Sum64 in checksum proto
		4) // checksum length
	estimatedSize := uint32(tb.curBlock.end) + uint32(6 /*header size for entry*/) +
		uint32(len(e.Key)) + uint32(e.EncodedSize()) + entriesOffsetsSize

	// Integer overflow check for table size.
	utils.CondPanic(!(uint64(tb.curBlock.end)+uint64(estimatedSize) < math.MaxUint32), errors.New("Integer overflow"))

	return estimatedSize > uint32(tb.opt.BlockSize)

}

func (tb *tableBuilder) finishBlock() {
	if tb.curBlock == nil || len(tb.curBlock.entryOffsets) == 0 {
		return
	}
	// Append the entryOffsets and its length
	tb.append(codec.U32SliceToBytes(tb.curBlock.entryOffsets))
	tb.append(codec.U32ToBytes(uint32(len(tb.curBlock.entryOffsets))))

	checksum := tb.calculateChecksum(tb.curBlock.data[:tb.curBlock.end])

	// Append the block checksum and its length
	tb.append(checksum)
	tb.append(codec.U32ToBytes(uint32(len(checksum))))

	tb.blockList = append(tb.blockList, tb.curBlock)

	tb.keyCount += uint32(len(tb.curBlock.entryOffsets))
	return
}

func (tb *tableBuilder) append(data []byte) {
	dst := tb.allocate(len(data))
	utils.CondPanic(len(data) != copy(dst, data), errors.New("tableBuilder.append data"))
}

func (tb *tableBuilder) allocate(need int) []byte {
	bb := tb.curBlock
	if len(bb.data[bb.end:]) < need {
		// We need to reallocate
		sz := 2 * len(bb.data)
		if bb.end+need > sz {
			sz = bb.end + need
		}
		tmp := make([]byte, sz)
		copy(tmp, bb.data)
		bb.data = tmp
	}
	bb.end += need
	return bb.data[bb.end-need : bb.end]
}

func (tb *tableBuilder) calculateChecksum(data []byte) []byte {
	checkSum := utils.CalculateChecksum(data)
	return codec.U64ToBytes(checkSum)
}

func (tb *tableBuilder) keyDiff(newKey []byte) []byte {
	var i int
	for i = 0; i < len(newKey) && i < len(tb.curBlock.baseKey); i++ {
		if newKey[i] != tb.curBlock.baseKey[i] {
			break
		}
	}
	return newKey[i:]
}

// TODO: 这里存在多次的用户空间拷贝过程，需要优化
func (tb *tableBuilder) flush(sst *file.SSTable) error {
	bd := tb.done()
	buf := make([]byte, bd.size)
	written := bd.Copy(buf)
	utils.CondPanic(written != len(buf), fmt.Errorf("tableBuilder.flush written != len(buf)"))
	dst, err := sst.Bytes(0, bd.size)
	if err != nil {
		return err
	}
	copy(dst, buf)
	return nil
}

func (bd *buildData) Copy(dst []byte) int {
	var written int
	for _, bl := range bd.blockList {
		written += copy(dst[written:], bl.data[:bl.end])
	}
	written += copy(dst[written:], bd.index)
	written += copy(dst[written:], codec.U32ToBytes(uint32(len(bd.index))))

	written += copy(dst[written:], bd.checksum)
	written += copy(dst[written:], codec.U32ToBytes(uint32(len(bd.checksum))))
	return written
}

func (tb *tableBuilder) done() buildData {
	tb.finishBlock()
	if len(tb.blockList) == 0 {
		return buildData{}
	}

	bd := buildData{
		blockList: tb.blockList,
	}

	var f utils.Filter
	if tb.opt.BloomFalsePositive > 0 {
		bits := utils.BloomBitsPerKey(len(tb.keyHashes), tb.opt.BloomFalsePositive)
		f = utils.NewFilter(tb.keyHashes, bits)
	}
	// TODO 构建sst的索引
	index, dataSize := tb.buildIndex(f)
	checksum := tb.calculateChecksum(index)
	bd.index = index
	bd.checksum = checksum
	bd.size = int(dataSize) + len(index) + len(checksum) + 4 + 4
	return bd
}

func (tb *tableBuilder) buildIndex(bloom []byte) ([]byte, uint32) {
	tableIndex := &pb.TableIndex{}
	if len(bloom) > 0 {
		tableIndex.BloomFilter = bloom
	}
	tableIndex.KeyCount = tb.keyCount
	tableIndex.MaxVersion = tb.maxVersion
	tableIndex.Offsets = tb.writeBlockOffsets(tableIndex)
	var dataSize uint32
	for i := range tb.blockList {
		dataSize += uint32(tb.blockList[i].end)
	}
	data, err := tableIndex.Marshal()
	utils.Panic(err)
	return data, dataSize
}

func (tb *tableBuilder) writeBlockOffsets(tableIndex *pb.TableIndex) []*pb.BlockOffset {
	var startOffset uint32
	var offsets []*pb.BlockOffset
	for _, bl := range tb.blockList {
		offset := tb.writeBlockOffset(bl, startOffset)
		offsets = append(offsets, offset)
		startOffset += uint32(bl.end)
	}
	return offsets
}

func (tb *tableBuilder) writeBlockOffset(bl *block, startOffset uint32) *pb.BlockOffset {
	offset := &pb.BlockOffset{}
	offset.Key = bl.baseKey
	offset.Len = uint32(bl.end)
	offset.Offset = startOffset
	return offset
}

func (b block) verifyCheckSum() error {
	return utils.VerifyChecksum(b.data, b.checksum)
}

type blockIterator struct {
	data         []byte
	idx          int
	err          error
	baseKey      []byte
	key          []byte
	val          []byte
	entryOffsets []uint32
	block        *block

	tableID uint64
	blockID int

	preOverlap uint16
	
	it         iterator.Item
}

func (itr *blockIterator) setBlock(b *block) {
	itr.block = b
	itr.err = nil
	itr.idx = 0
	itr.baseKey = itr.baseKey[:0]
	itr.preOverlap = 0
	itr.key = itr.key[:0]
	itr.val = itr.val[:0]
	// Drop the index from the block. We dont't need it anymore
	itr.data = b.data[:b.entriesIndexStart]
	itr.entryOffsets = b.entryOffsets
}

func (itr *blockIterator) seek(key []byte) {
	itr.err = nil
	startIndex := 0 // This tells from which index we should start binary search

	foundEntryIdx := sort.Search(len(itr.entryOffsets), func(idx int) bool {
		// If idx is less than start index the just return false
		if idx < startIndex {
			return false
		}
		itr.setIdx(idx)
		return utils.CompareKeys(itr.key, key) >= 0
	})
	itr.setIdx(foundEntryIdx)
}

func (itr *blockIterator) setIdx(i int) {
	itr.idx = i
	if i > len(itr.entryOffsets) || i < 0 {
		itr.err = io.EOF
		return
	}
	itr.err = nil
	startOffset := int(itr.entryOffsets[i])
	
}