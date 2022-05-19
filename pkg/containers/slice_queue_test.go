package containers

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSliceQueueBasics(t *testing.T) {
	q := NewSliceQueue[int]()

	require.False(t, checkSignal(q.C))
	require.Equal(t, 0, q.Size())
	q.Push(1)
	require.Equal(t, 1, q.Size())
	q.Push(2)
	require.Equal(t, 2, q.Size())
	q.Push(3)
	require.Equal(t, 3, q.Size())

	val, ok := q.Peek()
	require.True(t, ok)
	require.Equal(t, 1, val)

	require.True(t, checkSignal(q.C))
	val, ok = q.Pop()
	require.True(t, ok)
	require.Equal(t, 1, val)

	val, ok = q.Peek()
	require.True(t, ok)
	require.Equal(t, 2, val)

	require.True(t, checkSignal(q.C))
	val, ok = q.Pop()
	require.True(t, ok)
	require.Equal(t, 2, val)

	val, ok = q.Peek()
	require.True(t, ok)
	require.Equal(t, 3, val)

	require.True(t, checkSignal(q.C))
	val, ok = q.Pop()
	require.True(t, ok)
	require.Equal(t, 3, val)

	require.False(t, checkSignal(q.C))
	_, ok = q.Pop()
	require.False(t, ok)

	_, ok = q.Peek()
	require.False(t, ok)
}

func TestSliceQueueManyElements(t *testing.T) {
	const numElems = 10000

	q := NewSliceQueue[int]()
	for i := 0; i < numElems; i++ {
		q.Push(i)
	}
	require.Equal(t, numElems, q.Size())

	for i := 0; i < numElems; i++ {
		val, ok := q.Pop()
		require.True(t, ok)
		require.Equal(t, i, val)
	}
	require.Equal(t, 0, q.Size())

	// Repeat the test
	for i := 0; i < numElems; i++ {
		q.Push(i)
	}
	require.Equal(t, numElems, q.Size())

	for i := 0; i < numElems; i++ {
		val, ok := q.Pop()
		require.True(t, ok)
		require.Equal(t, i, val)
	}
	require.Equal(t, 0, q.Size())
}

func TestSliceQueueConcurrentWriteAndRead(t *testing.T) {
	const numElems = 1000000

	q := NewSliceQueue[int]()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < numElems; i++ {
			q.Push(i)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		counter := 0
		for {
			select {
			case <-q.C:
			}

			for {
				val, ok := q.Pop()
				if !ok {
					break
				}
				require.Equal(t, counter, val)
				counter++
				if counter == numElems {
					return
				}
			}
		}
	}()

	wg.Wait()
	require.Equal(t, 0, q.Size())
}

func checkSignal(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
