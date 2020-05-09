package notify

import (
	"context"
	"sync"
	"time"
)

// Notifier provides a one-to-many notification mechanism
type Notifier struct {
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
		signalNonBlocking(ctx, notifyCh.ch)
	}
}

func signalNonBlocking(ctx context.Context, ch chan struct{}) {
	select {
	case <-ctx.Done():
	case ch <- struct{}{}:
	default:
	}
}

type Receiver struct {
	C    <-chan struct{}
	Stop func()
}

// NewReceiver creates a receiver
// returns a channel to receive notifications and a function to close this receiver
func (n *Notifier) NewReceiver(ctx context.Context, tickTime time.Duration) *Receiver {
	n.mu.Lock()
	defer n.mu.Unlock()
	receiverCh := make(chan struct{}, 1)
	currentIndex := n.maxIndex
	n.maxIndex++
	n.notifyChs = append(n.notifyChs, struct {
		ch    chan struct{}
		index int
	}{ch: receiverCh, index: currentIndex})
	stopTicker := func() {}
	if tickTime > 0 {
		ticker := time.NewTicker(tickTime)
		stopTicker = ticker.Stop
		go func() {
			for range ticker.C {
				signalNonBlocking(ctx, receiverCh)
			}
		}()
	}
	return &Receiver{
		C: receiverCh,
		Stop: func() {
			stopTicker()
			n.remove(currentIndex)
		},
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
