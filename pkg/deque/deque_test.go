package deque

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeque(t *testing.T) {
	deque := NewDeque[int](2, 5)

	// Test empty deque
	assert.Equal(t, 0, deque.Length())

	// Test PushBack
	deque.PushBack(1)
	deque.PushBack(2)
	deque.PushBack(3)
	assert.Equal(t, 3, deque.Length())
	back, ok := deque.Back()
	assert.True(t, ok)
	assert.Equal(t, 3, back)

	// Test PushFront
	deque.PushFront(0)
	assert.Equal(t, 4, deque.Length())
	front, ok := deque.Front()
	assert.True(t, ok)
	assert.Equal(t, 0, front)

	// Test PopBack
	item, ok := deque.PopBack()
	assert.True(t, ok)
	assert.Equal(t, 3, item)
	assert.Equal(t, 3, deque.Length())

	// Test PopFront
	item, ok = deque.PopFront()
	assert.True(t, ok)
	assert.Equal(t, 0, item)
	assert.Equal(t, 2, deque.Length())

	deque.PopFront()
	deque.PopFront()
	item, ok = deque.PopFront()
	assert.False(t, ok)
	assert.Equal(t, 0, item)
	assert.Equal(t, 0, deque.Length())

	deque.PushFront(1)
	deque.PushFront(2)
	deque.PushFront(3)
	deque.PushFront(4)
	deque.PushFront(5)
	deque.PushFront(6)
	assert.Equal(t, 5, deque.Length())
	item, ok = deque.Back()
	assert.True(t, ok)
	assert.Equal(t, 2, item)

	// Test Forward and Backward Iterator
	{
		itr := deque.ForwardIterator()
		items := make([]int, 0, deque.Length())
		for item, ok := itr.Next(); ok; item, ok = itr.Next() {
			items = append(items, item)
		}
		assert.Equal(t, []int{6, 5, 4, 3, 2}, items)
	}

	{
		itr := deque.BackwardIterator()
		items := make([]int, 0, deque.Length())
		for item, ok := itr.Next(); ok; item, ok = itr.Next() {
			items = append(items, item)
		}
		assert.Equal(t, []int{2, 3, 4, 5, 6}, items)
	}
}
