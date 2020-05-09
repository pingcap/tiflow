package notify

import (
	"context"
	"sync"
)

// GlobalNotifyHub is a notify hub which is global level
var GlobalNotifyHub = NewNotifyHub()

// Hub is a notify manager, used to create and maintain Notifier objects
type Hub struct {
	notifiers map[string]*Notifier
	mu        sync.Mutex
}

// NewNotifyHub creates the NotifyHub
func NewNotifyHub() *Hub {
	return &Hub{
		notifiers: make(map[string]*Notifier),
	}
}

// GetNotifier gets the Notifier corresponding to the name
// if the Notifier is not exists, this method will create a new one.
func (n *Hub) GetNotifier(name string) *Notifier {
	n.mu.Lock()
	defer n.mu.Unlock()
	if notifier, ok := n.notifiers[name]; ok {
		return notifier
	}
	notifier := &Notifier{
		name: name,
	}
	n.notifiers[name] = notifier
	return notifier
}

// CloseNotifier closes the Notifier corresponding to the name
func (n *Hub) CloseNotifier(name string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if notifier, ok := n.notifiers[name]; ok {
		notifier.close()
		delete(n.notifiers, name)
	}
}

// CloseAll closes all notifiers
func (n *Hub) CloseAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for name, notifier := range n.notifiers {
		notifier.close()
		delete(n.notifiers, name)
	}
}

// Notifier provides a one-to-many notification mechanism
type Notifier struct {
	name      string
	notifyChs []struct {
		ch    chan struct{}
		index int
	}
	maxIndex int
	mu       sync.RWMutex
}

// Notify sends a signal to the Receivers
func (n *Notifier) Notify(ctx context.Context) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, notifyCh := range n.notifyChs {
		select {
		case <-ctx.Done():
		case notifyCh.ch <- struct{}{}:
		default:
		}
	}
}

// Receiver creates a receiver
// returns a channel to receive notifications and a function to close this receiver
func (n *Notifier) Receiver() (<-chan struct{}, func()) {
	n.mu.Lock()
	defer n.mu.Unlock()
	receiverCh := make(chan struct{}, 1)
	currentIndex := n.maxIndex
	n.maxIndex++
	n.notifyChs = append(n.notifyChs, struct {
		ch    chan struct{}
		index int
	}{ch: receiverCh, index: currentIndex})
	return receiverCh, func() {
		n.remove(currentIndex)
	}
}

func (n *Notifier) remove(index int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i, notifyCh := range n.notifyChs {
		if notifyCh.index == index {
			close(notifyCh.ch)
			n.notifyChs = append(n.notifyChs[:i], n.notifyChs[i+1:]...)
			break
		}
	}
}

func (n *Notifier) close() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, notifyCh := range n.notifyChs {
		close(notifyCh.ch)
	}
}
