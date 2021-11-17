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
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/hardcore-os/corekv/file"
	"github.com/hardcore-os/corekv/iterator"
	"github.com/hardcore-os/corekv/utils"
	"github.com/hardcore-os/corekv/utils/codec"
	"github.com/hardcore-os/corekv/utils/codec/pb"
)

// TODO LAB 这里实现 table

type table struct {
	ss  *file.SSTable
	lm  *levelManager
	fid uint64
}

func openTable(lm *levelManager, tableName string, builder *tableBuilder) *table {
	ss := file.OpenSStable(&file.Options{
		FileName: tableName,
		Dir:      lm.opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(lm.opt.SSTableMaxSz)})
	t := &table{ss: ss, lm: lm, fid: utils.FID(tableName)}
	//对builder来flush到此篇
	if builder != nil {
		if err := builder.flush(ss); err != nil {
			utils.Err(err)
			return nil
		}
	}
	if err := t.ss.Init(); err != nil {
		utils.Err(err)
	}
	return t
}

// Search 从table中查询key
func (t *table) Search(key []byte, maxVs *uint64) (entry *codec.Entry, err error) {
	// 获取索引
	idx := t.ss.Indexs()
	// 检查key是否存在
	bloomFilter := utils.Filter(idx.BloomFilter)
	if t.ss.HasBloomFilter() && !bloomFilter.MayContainKey(key) {
		return nil, utils.ErrKeyNotFound
	}

	iter := t.NewIterator(&iterator.Options{})
	defer iter.Close()

	iter.Seek(key)
	if iter.Valid() {
		return nil, utils.ErrKeyNotFound
	}

	if codec.SameKey(key, iter.Item().Entry().Key) {
		if version := codec.ParseTs(iter.Item().Entry().Key); *maxVs < version {
			*maxVs = version
			return iter.Item().Entry(), nil
		}
	}

	return nil, utils.ErrKeyNotFound

}

type tableIterator struct {
	it       iterator.Item
	opt      *iterator.Options
	t        *table
	blockPos int
	bi       *blockIterator
	err      error
}

func (t *table) NewIterator(options *iterator.Options) iterator.Iterator {
	return &tableIterator{
		opt: options,
		t:   t,
		bi:  &blockIterator{},
	}
}

func (it *tableIterator) Next() {

}

func (it *tableIterator) Valid() bool {
	return it == nil
}

func (it *tableIterator) Rewind() {

}

func (it *tableIterator) Item() iterator.Item {
	return it.it
}

func (it *tableIterator) Close() error {
	return nil
}

// Seek
// 二分搜索法 offsets
// 如果idx == 0 说明只能在第一个block中 block[0].MinKey <= key
// 否则block[0].MinKey > key
// 如果在idx-1的block中未找到key 那才能在idx中
// 如果都没有，则当前key不在此table
func (it *tableIterator) Seek(key []byte) {
	var ko pb.BlockOffset
	idx := sort.Search(len(it.t.ss.Indexs().GetOffsets()), func(idx int) bool {
		utils.CondPanic(!it.t.offsets(&ko, idx), fmt.Errorf("tableutils.Seek idx < 0 || idx > len(index.GetOffsets()"))
		return utils.CompareKeys(ko.GetKey(), key) > 0
	})
	if idx == 0 {
		it.seekHelper(0, key)
		return
	}
	it.seekHelper(idx-1, key)
	if it.err == io.EOF {
		if idx == len(it.t.ss.Indexs().Offsets) {
			return
		}
		it.seekHelper(idx, key)
	}

}

func (it *tableIterator) seekHelper(blockIdx int, key []byte) {
	it.blockPos = blockIdx
	block, err := it.t.block(blockIdx)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seek(key)
	it.err = it.bi.Error()
	it.it = it.bi.Item()
}

func (t *table) offsets(ko *pb.BlockOffset, i int) bool {
	index := t.ss.Indexs()
	if i < 0 || i > len(index.GetOffsets()) {
		return false
	}
	*ko = *index.GetOffsets()[i]
	return true
}
