package dm

import (
	"context"
	"sync"

	"github.com/pingcap/errors"

	"github.com/hanfei1991/microcosm/pkg/p2p"
)

// MessageIDAllocator is an id allocator for p2p message system
type MessageIDAllocator struct {
	mu sync.Mutex
	id uint64
}

// Alloc allocs a new message id
func (a *MessageIDAllocator) Alloc() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.id++
	return a.id
}

// Sender defines an interface that supports send message
type Sender interface {
	SendMessage(ctx context.Context, topic p2p.Topic, message interface{}, nonblocking bool) error
}

// MessagePair implement a simple synchronous request/response message pattern since the lib currently only support asynchronous message.
// Caller should persist the request message if needed.
// Caller should add retry mechanism if needed.
type MessagePair struct {
	// messageID -> response channel
	// TODO: limit the MaxPendingMessageCount if needed.
	pendings    sync.Map
	idAllocator *MessageIDAllocator
}

// NewMessagePair creates a new MessagePair instance
func NewMessagePair() *MessagePair {
	return &MessagePair{
		idAllocator: &MessageIDAllocator{},
	}
}

// SendRequest sends a request message and wait for response.
func (m *MessagePair) SendRequest(ctx context.Context, topic p2p.Topic, req Request, sender Sender) (interface{}, error) {
	msg := MessageWithID{ID: m.idAllocator.Alloc(), Message: req}
	respCh := make(chan Response, 1)
	m.pendings.Store(msg.ID, respCh)
	defer m.pendings.Delete(msg.ID)

	if err := sender.SendMessage(ctx, topic, msg, false /* block */); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-respCh:
		return resp, nil
	}
}

// OnResponse receives a response message.
func (m *MessagePair) OnResponse(msg MessageWithID) error {
	respCh, ok := m.pendings.Load(msg.ID)
	if !ok {
		return errors.Errorf("request %d not found", msg.ID)
	}

	select {
	case respCh.(chan Response) <- msg.Message:
		return nil
	default:
	}
	return errors.Errorf("duplicated response of request %d, and the last response is not consumed", msg.ID)
}
