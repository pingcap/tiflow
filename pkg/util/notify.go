package util

import (
	"context"
	"sync"
)

var GlobalNotifyHub = NewNotifyHub()

type NotifyHub struct {
	notifiers map[string]*Notifier
	mu        sync.Mutex
}

func NewNotifyHub() *NotifyHub {
	return &NotifyHub{
		notifiers: make(map[string]*Notifier),
	}
}

func (n *NotifyHub) GetNotifier(name string) *Notifier {
	n.mu.Lock()
	defer n.mu.Unlock()
	if notifier, ok := n.notifiers[name]; ok {
		return notifier
	} else {
		notifier := &Notifier{
			name: name,
		}
		n.notifiers[name] = notifier
		return notifier
	}
}

func (n *NotifyHub) CloseNotifier(name string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if notifier, ok := n.notifiers[name]; ok {
		notifier.close()
		delete(n.notifiers, name)
	}
}

func (n *NotifyHub) CloseAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for name, notifier := range n.notifiers {
		notifier.close()
		delete(n.notifiers, name)
	}
}

type Notifier struct {
	name      string
	notifyChs []struct {
		ch    chan struct{}
		index int
	}
	maxIndex int
	mu       sync.RWMutex
}

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
