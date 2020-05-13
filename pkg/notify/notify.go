package notify

import (
	"sync"
	"sync/atomic"
	"time"
)

// Notifier provides a one-to-many notification mechanism
type Notifier struct {
	receivers []struct {
		rec   *Receiver
		index int
	}
	maxIndex int
	mu       sync.RWMutex
}

// Notify sends a signal to the Receivers
func (n *Notifier) Notify() {
	n.mu.RLock()
	defer n.mu.RUnlock()
	for _, receiver := range n.receivers {
		receiver.rec.notify()
	}
}

// Receiver is a receiver of notifier, including the receiver channel and stop receiver function.
type Receiver struct {
	C      <-chan struct{}
	Stop   func()
	ticker *time.Ticker
	c      chan struct{}
	closed int32
}

func (r *Receiver) notify() {
	if atomic.LoadInt32(&r.closed) != 0 {
		return
	}
	select {
	case r.c <- struct{}{}:
	default:
	}
}

// NewReceiver creates a receiver
// returns a channel to receive notifications and a function to close this receiver
func (n *Notifier) NewReceiver(tickTime time.Duration) *Receiver {
	n.mu.Lock()
	defer n.mu.Unlock()
	currentIndex := n.maxIndex
	n.maxIndex++
	receiverCh := make(chan struct{}, 1)
	rec := &Receiver{
		C: receiverCh,
		Stop: func() {
			n.remove(currentIndex)
		},
		c: receiverCh,
	}
	if tickTime > 0 {
		rec.ticker = time.NewTicker(tickTime)
		go func() {
			for range rec.ticker.C {
				rec.notify()
			}
		}()
	}
	n.receivers = append(n.receivers, struct {
		rec   *Receiver
		index int
	}{rec: rec, index: currentIndex})
	return rec
}

func (n *Notifier) remove(index int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i, receiver := range n.receivers {
		if receiver.index == index {
			atomic.StoreInt32(&receiver.rec.closed, 1)
			n.receivers = append(n.receivers[:i], n.receivers[i+1:]...)
			if receiver.rec.ticker != nil {
				receiver.rec.ticker.Stop()
			}
			close(receiver.rec.c)
			break
		}
	}
}

// Close closes the notify and stops all receiver in this notifier
func (n *Notifier) Close() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, receiver := range n.receivers {
		if receiver.rec.ticker != nil {
			receiver.rec.ticker.Stop()
		}
		close(receiver.rec.c)
	}
	n.receivers = nil
}
