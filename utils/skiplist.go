package utils

import (
	"bytes"
	"github.com/hardcore-os/corekv/fastrand"
	"math"
	"sync"
	"sync/atomic"
)

const (
	defaultMaxLevel = 20
	pValue          = 1 / math.E
)

var (
	probabilities [defaultMaxLevel]uint32
)

func init() {
	p := float64(1.0)
	for i := 0; i < defaultMaxLevel; i++ {
		probabilities[i] = uint32(float64(math.MaxUint32) * p)
		p *= pValue
	}
}

type SkipList struct {
	lock       sync.RWMutex //读写锁，用来实现并发安全的sl
	currHeight uint32       //sl当前的最大高度
	headOffset uint32       //头结点在arena当中的偏移量
	arena      *Arena
}

func NewSkipList(arenaSize int64) *SkipList {
	arena := newArena(arenaSize)
	//引入一个空的头结点，因此Key和Value都是空的
	head := newElement(arena, nil, ValueStruct{}, defaultMaxLevel)
	ho := arena.getElementOffset(head)

	return &SkipList{
		currHeight: 1,
		headOffset: ho,
		arena:      arena,
	}
}

func newElement(arena *Arena, key []byte, v ValueStruct, height int) *Element {
	nodeOffset := arena.putNode(height)

	keyOffset := arena.putKey(key)
	val := encodeValue(arena.putVal(v), v.EncodedSize())

	elem := arena.getElement(nodeOffset) //这里的elem是根据内存中的地址来读取的，不是arena中的offset
	elem.score = calcScore(key)
	elem.keyOffset = keyOffset
	elem.keySize = uint16(len(key))
	elem.height = uint16(height)
	elem.value = val

	return elem
}

//用来对value值进行编解码
//value = valueSize | valueOffset
func encodeValue(valOffset uint32, valSize uint32) uint64 {
	return uint64(valSize)<<32 | uint64(valOffset)
}

func decodeValue(value uint64) (valOffset uint32, valSize uint32) {
	valOffset = uint32(value)
	valSize = uint32(value >> 32)
	return
}

type Element struct {
	score float64 //加快查找，只在内存中生效，因此不需要持久化
	value uint64  //将value的off和size组装成一个uint64，实现原子化的操作

	keyOffset uint32
	keySize   uint16

	height uint16

	levels [defaultMaxLevel]uint32 //这里先按照最大高度声明，往arena中放置的时候，会计算实际高度和内存消耗
}

func (e *Element) key(arena *Arena) []byte {
	return arena.getKey(e.keyOffset, e.keySize)
}

func (e *Element) getValueOffset() (uint32, uint32) {
	value := atomic.LoadUint64(&e.value)
	return decodeValue(value)

}

func (list *SkipList) Size() int64 {
	return list.arena.Size()
}

func (list *SkipList) getHeight() uint32 {
	return atomic.LoadUint32(&list.currHeight)
}

func (list *SkipList) findSpliceForLevel(score float64, key []byte, before uint32, level int) (uint32, uint32) {
	for {
		// Assume before.key < key.
		beforeNode := list.arena.getElement(before)
		next := beforeNode.getNextOffset(level)
		nextNode := list.arena.getElement(next)

		if nextNode == nil {
			return before, next
		}

		cmp := list.compare(score, key, nextNode)

		if cmp == 0 {
			// Equality case.
			return next, next
		}
		if cmp < 0 {
			// before.key < key < next.key. We are done for this level.
			return before, next
		}
		before = next // Keep moving right on this level.
	}
}

// findNear finds the node near to key.
// If less=true, it finds rightmost node such that node.key < key (if allowEqual=false) or
// node.key <= key (if allowEqual=true).
// If less=false, it finds leftmost node such that node.key > key (if allowEqual=false) or
// node.key >= key (if allowEqual=true).
// Returns the node found. The bool returned is true if the node has key equal to given key.
func (list *SkipList) findNear(score float64, key []byte, less bool, allowEqual bool) (*Element, bool) {
	x := list.getHead()
	level := int(list.getHeight() - 1)
	for {
		// Assume x.key < key
		next := list.getNext(x, level)
		if next == nil {
			// x.key < key < next.key
			if level > 0 {
				// can descend further to iterate closer to the end.
				level--
				continue
			}
			// level == 0. cannot descend further. Let's return something that makes sense.
			if !less {
				return nil, false
			}
			// try to return x. Make sure it is not a head node.
			if x == list.getHead() {
				return nil, false
			}
			return x, false
		}

		cmp := list.compare(score, key, next)
		if cmp > 0 {
			// x.key < next.key < key . we can continue to move right.
			x = next
			continue
		}
		if cmp == 0 {
			// x.key < key == next.key
			if allowEqual == true {
				return next, true
			}
			if !less {
				// we want >, so go to base level to grab the next bigger note.
				return list.getNext(next, 0), false
			}

			// we want <. If not base level, we should go closer in the next level.
			if level > 0 {
				level--
				continue
			}
			// on base level. Return x.
			if x == list.getHead() {
				return nil, false
			}
			return x, false
		}
		// cmp < 0. In other words, x.key < key < next
		if level > 0 {
			level--
			continue
		}
		// at base level. need to return something.
		if !less {
			return next, false
		}
		// try to return x. make sure it is not a head node.
		if x == list.getHead() {
			return nil, false
		}
		return x, false
	}
}

func (list *SkipList) Add(data *Entry) error {
	list.lock.Lock()
	defer list.lock.Unlock()
	score := calcScore(data.Key)
	var elem *Element
	value := ValueStruct{
		Value: data.Value,
	}

	//从当前最大高度开始
	listHeight := list.getHeight()

	//用来记录访问路径
	var prev [defaultMaxLevel + 1]uint32
	var next [defaultMaxLevel + 1]uint32
	prev[listHeight] = list.headOffset

	for i := int(listHeight) - 1; i >= 0; i-- {

		prev[i], next[i] = list.findSpliceForLevel(score, data.Key, prev[i+1], i)

		if prev[i] == next[i] {
			vo := list.arena.putVal(value)
			encV := encodeValue(vo, value.EncodedSize())
			prevNode := list.arena.getElement(prev[i])
			prevNode.value = encV
			return nil
		}
	}

	//level := list.randLevel()
	level := list.randomHeight()

	if level > listHeight {
		list.currHeight = level
	}

	elem = newElement(list.arena, data.Key, ValueStruct{Value: data.Value}, int(level))
	//to add elem to the skiplist
	off := list.arena.getElementOffset(elem)
	for i := 0; i < int(level); i++ {
		if list.arena.getElement(prev[i]) == nil {
			AssertTrue(i > 1) // This cannot happen in base level.
			// We haven't computed prev, next for this level because height exceeds old listHeight.
			// For these levels, we expect the lists to be sparse, so we can just search from head.
			prev[i], next[i] = list.findSpliceForLevel(score, data.Key, list.headOffset, i)
			// Someone adds the exact same key before we are able to do so. This can only happen on
			// the base level. But we know we are not on the base level.
			AssertTrue(prev[i] != next[i])
		}
		elem.levels[i] = next[i]
		pnode := list.arena.getElement(prev[i])
		pnode.levels[i] = off
	}

	return nil
}

func (list *SkipList) Search(key []byte) (e *Entry) {
	list.lock.RLock()
	defer list.lock.RUnlock()
	score := calcScore(key)
	n, _ := list.findNear(score, key, false, true)
	if n == nil {
		return nil
	}

	nextKey := list.arena.getKey(n.keyOffset, n.keySize)
	if !SameKey(key, nextKey) {
		return nil
	}

	valOffset, valSize := n.getValueOffset()
	return &Entry{Key: key, Value: list.arena.getVal(valOffset, valSize).Value}

}

func (list *SkipList) Close() error {
	return nil
}

func calcScore(key []byte) (score float64) {
	var hash uint64
	l := len(key)

	if l > 8 {
		l = 8
	}

	for i := 0; i < l; i++ {
		shift := uint(64 - 8 - i*8)
		hash |= uint64(key[i]) << shift
	}

	score = float64(hash)
	return
}

func (list *SkipList) compare(score float64, key []byte, next *Element) int {
	if score == next.score {
		return bytes.Compare(key, next.key(list.arena))
	}

	if score < next.score {
		return -1
	} else {
		return 1
	}
}

func (s *SkipList) randomHeight() uint32 {
	rnd := fastrand.Uint32()
	h := uint32(1)
	for h < defaultMaxLevel && rnd <= probabilities[h] {
		h++
	}
	return h
}

//func (list *SkipList) randLevel() int {
//	if list.maxLevel <= 1 {
//		return 1
//	}
//	i := 1
//	for ; i < list.maxLevel; i++ {
//		if RandN(1000)%2 == 0 {
//			return i
//		}
//	}
//	return i
//}

//拿到某个节点，在某个高度上的next节点
//如果该节点已经是该层最后一个节点（该节点的level[height]将是0），会返回nil
func (list *SkipList) getNext(e *Element, height int) *Element {
	return list.arena.getElement(e.getNextOffset(height))
}

func (list *SkipList) getHead() *Element {
	return list.arena.getElement(list.headOffset)
}

type SkipListIter struct {
	list *SkipList
	elem *Element //iterator当前持有的节点
	lock sync.RWMutex
}

func (list *SkipList) NewSkipListIterator() Iterator {
	return &SkipListIter{
		list: list,
	}
}

func (iter *SkipListIter) Next() {
	AssertTrue(iter.Valid())
	iter.elem = iter.list.getNext(iter.elem, 0) //只在最底层遍历就行了
}

func (iter *SkipListIter) Valid() bool {
	return iter.elem != nil
}
func (iter *SkipListIter) Rewind() {
	head := iter.list.arena.getElement(iter.list.headOffset)
	iter.elem = iter.list.getNext(head, 0)
}

func (iter *SkipListIter) Item() Item {
	vo, vs := decodeValue(iter.elem.value)
	return &Entry{
		Key:       iter.list.arena.getKey(iter.elem.keyOffset, iter.elem.keySize),
		Value:     iter.list.arena.getVal(vo, vs).Value,
		ExpiresAt: iter.list.arena.getVal(vo, vs).ExpiresAt,
	}
}
func (iter *SkipListIter) Close() error {
	return nil
}

func (iter *SkipListIter) Seek(key []byte) {
}
