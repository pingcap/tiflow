package deque

import (
	"github.com/pingcap/tiflow/pkg/list"
)

// A deque implemented by a doubly linked list of fixed-size blocks.
type Deque[T any] struct {
	blockLen int
	maxLen   int
	zero     T

	blocks *list.List[[]T]
	length int

	// Those indexes point to the first and last available value of the deque.
	front int
	back  int
}

func NewDequeDefault[T any]() *Deque[T] {
	return NewDeque[T](128, 0)
}

// blockLen is the size of each block.
// maxLen is the maximum length of the deque. If the length exceeds maxLen, the oldest values will be removed. Zero means no limit.
func NewDeque[T any](blockLen int, maxLen int) *Deque[T] {
	if blockLen < 2 {
		panic("blockLen must be at least 2")
	}
	d := &Deque[T]{
		blockLen: blockLen,
		maxLen:   maxLen,
		blocks:   list.NewList[[]T](),
	}
	d.blocks.PushBack(make([]T, blockLen))
	d.resetEmpty()
	return d
}

func (d *Deque[T]) Length() int {
	return d.length
}

func (d *Deque[T]) resetEmpty() {
	d.front = d.blockLen / 2
	d.back = d.blockLen/2 - 1
}

func (d *Deque[T]) Back() (T, bool) {
	if d.length == 0 {
		return d.zero, false
	}
	return d.blocks.Back().Value[d.back], true
}

func (d *Deque[T]) Front() (T, bool) {
	if d.length == 0 {
		return d.zero, false
	}
	return d.blocks.Front().Value[d.front], true
}

func (d *Deque[T]) PushBack(item T) {
	block := d.blocks.Back().Value
	if d.back == d.blockLen-1 {
		// the last block is full
		block = make([]T, d.blockLen)
		d.blocks.PushBack(block)
		d.back = -1
	}

	d.back++
	block[d.back] = item
	d.length++

	if d.maxLen > 0 && d.length > d.maxLen {
		d.PopFront()
	}
}

func (d *Deque[T]) PopBack() (T, bool) {
	if d.length == 0 {
		return d.zero, false
	}

	le := d.blocks.Back()
	block := le.Value
	item := block[d.back]
	block[d.back] = d.zero
	d.back--
	d.length--

	if d.back == -1 {
		// The current blocks is drained
		if d.length == 0 {
			d.resetEmpty()
		} else {
			d.blocks.Remove(le)
			d.back = d.blockLen - 1
		}
	}

	return item, true
}

func (d *Deque[T]) PushFront(item T) {
	block := d.blocks.Front().Value
	if d.front == 0 {
		// the first block is full
		block = make([]T, d.blockLen)
		d.blocks.PushFront(block)
		d.front = d.blockLen
	}

	d.front--
	block[d.front] = item
	d.length++

	if d.maxLen > 0 && d.length > d.maxLen {
		d.PopBack()
	}
}

func (d *Deque[T]) PopFront() (T, bool) {
	if d.length == 0 {
		return d.zero, false
	}

	le := d.blocks.Front()
	block := le.Value
	item := block[d.front]
	block[d.front] = d.zero
	d.front++
	d.length--

	if d.front == d.blockLen {
		// The current blocks is drained
		if d.length == 0 {
			d.resetEmpty()
		} else {
			d.blocks.Remove(le)
			d.front = 0
		}
	}

	return item, true
}

type ForwardIter[T any] struct {
	blocks *list.List[[]T]
	length int

	front int
	back  int
}

func (d *Deque[T]) ForwardIterator() *ForwardIter[T] {
	copyBlocks := list.NewList[[]T]()
	for e := d.blocks.Front(); e != nil; e = e.Next() {
		copyBlocks.PushBack(e.Value)
	}
	return &ForwardIter[T]{
		blocks: copyBlocks,
		length: d.length,
		front:  d.front,
		back:   d.back,
	}
}

func (it *ForwardIter[T]) Next() (T, bool) {
	var zero T
	if it.length == 0 {
		return zero, false
	}

	block := it.blocks.Front().Value
	item := block[it.front]
	it.front++
	it.length--

	if it.front == len(block) {
		it.blocks.Remove(it.blocks.Front())
		it.front = 0
	}

	return item, true
}

type BackwardIter[T any] struct {
	blocks *list.List[[]T]
	length int

	front int
	back  int
}

func (d *Deque[T]) BackwardIterator() *BackwardIter[T] {
	copyBlocks := list.NewList[[]T]()
	for e := d.blocks.Front(); e != nil; e = e.Next() {
		copyBlocks.PushBack(e.Value)
	}

	return &BackwardIter[T]{
		blocks: copyBlocks,
		length: d.length,
		front:  d.front,
		back:   d.back,
	}
}

func (it *BackwardIter[T]) Next() (T, bool) {
	var zero T
	if it.length == 0 {
		return zero, false
	}

	block := it.blocks.Back().Value
	item := block[it.back]
	it.back--
	it.length--

	if it.back == -1 {
		it.blocks.Remove(it.blocks.Back())
		it.back = len(block) - 1
	}

	return item, true
}
