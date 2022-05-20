package dm

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/stretchr/testify/require"
)

func TestMessageIDAllocator(t *testing.T) {
	t.Parallel()

	allocator := &MessageIDAllocator{}
	require.Equal(t, uint64(1), allocator.Alloc())
	require.Equal(t, uint64(2), allocator.Alloc())
	require.Equal(t, uint64(3), allocator.Alloc())

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				allocator.Alloc()
			}
		}()
	}
	wg.Wait()
	require.Equal(t, uint64(1004), allocator.Alloc())
}

func TestMessagePair(t *testing.T) {
	t.Parallel()

	messagePair := NewMessagePair()
	mockSender := &MockSender{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	messageErr := errors.New("message error")
	// synchronous send
	mockSender.SetResult([]error{messageErr})
	resp, err := messagePair.SendRequest(ctx, "topic", "request", mockSender)
	require.EqualError(t, err, messageErr.Error())
	require.Nil(t, resp)
	mockSender.ClearMessage()
	// deadline exceeded
	mockSender.SetResult([]error{nil})
	ctx2, cancel2 := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel2()
	resp, err = messagePair.SendRequest(ctx2, "topic", "request", mockSender)
	require.EqualError(t, err, context.DeadlineExceeded.Error())
	require.Nil(t, resp)
	// late response
	msg := mockSender.PopMessage().(MessageWithID)
	require.EqualError(t, messagePair.OnResponse(MessageWithID{ID: msg.ID, Message: "response"}), fmt.Sprintf("request %d not found", msg.ID))

	go func() {
		mockSender.SetResult([]error{nil})
		ctx3, cancel3 := context.WithTimeout(ctx, 5*time.Second)
		defer cancel3()
		resp, err := messagePair.SendRequest(ctx3, "topic", "request", mockSender)
		require.NoError(t, err)
		require.Equal(t, "response", resp)
	}()
	require.Eventually(t, func() bool {
		return mockSender.Len() == 1
	}, 5*time.Second, 100*time.Millisecond)
	msg = mockSender.PopMessage().(MessageWithID)
	require.NoError(t, messagePair.OnResponse(MessageWithID{ID: msg.ID, Message: "response"}))

	// duplicate response
	require.Eventually(t, func() bool {
		err := messagePair.OnResponse(MessageWithID{ID: msg.ID, Message: "response"})
		return err != nil && err.Error() == fmt.Sprintf("request %d not found", msg.ID)
	}, 5*time.Second, 100*time.Millisecond)
}

type MockSender struct {
	sync.Mutex
	results      []error
	messageQueue []interface{}
}

func (s *MockSender) SetResult(results []error) {
	s.Lock()
	defer s.Unlock()
	s.results = append(s.results, results...)
}

func (s *MockSender) SendMessage(ctx context.Context, topic p2p.Topic, message interface{}, nonblocking bool) error {
	s.Lock()
	defer s.Unlock()
	if len(s.results) == 0 {
		panic("no result in mock sender")
	}
	result := s.results[0]
	s.results = s.results[1:]
	s.messageQueue = append(s.messageQueue, message)
	return result
}

func (s *MockSender) PopMessage() interface{} {
	s.Lock()
	defer s.Unlock()
	if len(s.messageQueue) == 0 {
		panic("no message in mock sender")
	}
	message := s.messageQueue[0]
	s.messageQueue = s.messageQueue[1:]
	return message
}

func (s *MockSender) Len() int {
	s.Lock()
	defer s.Unlock()
	return len(s.messageQueue)
}

func (s *MockSender) ClearMessage() {
	s.Lock()
	defer s.Unlock()
	s.messageQueue = []interface{}{}
}
