// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package notify

import (
	"sync"
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
		receiver.rec.signalNonBlocking()
	}
}

// Receiver is a receiver of notifier, including the receiver channel and stop receiver function.
type Receiver struct {
	C       <-chan struct{}
	c       chan struct{}
	Stop    func()
	ticker  *time.Ticker
	closeCh chan struct{}
}

// returns true if the receiverCh should be closed
func (r *Receiver) signalNonBlocking() bool {
	select {
	case <-r.closeCh:
		return true
	case r.c <- struct{}{}:
	default:
	}
	return false
}

func (r *Receiver) signalTickLoop() {
	go func() {
	loop:
		for range r.ticker.C {
			exit := r.signalNonBlocking()
			if exit {
				break loop
			}
		}
		close(r.c)
	}()
}

// NewReceiver creates a receiver
// returns a channel to receive notifications and a function to close this receiver
func (n *Notifier) NewReceiver(tickTime time.Duration) *Receiver {
	n.mu.Lock()
	defer n.mu.Unlock()
	currentIndex := n.maxIndex
	n.maxIndex++
	receiverCh := make(chan struct{}, 1)
	closeCh := make(chan struct{})
	var ticker *time.Ticker
	if tickTime > 0 {
		ticker = time.NewTicker(tickTime)
	}
	rec := &Receiver{
		C: receiverCh,
		c: receiverCh,
		Stop: func() {
			n.remove(currentIndex)
		},
		ticker:  ticker,
		closeCh: closeCh,
	}
	if tickTime > 0 {
		rec.signalTickLoop()
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
			n.receivers = append(n.receivers[:i], n.receivers[i+1:]...)
			close(receiver.rec.closeCh)
			if receiver.rec.ticker != nil {
				receiver.rec.ticker.Stop()
			}
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
		close(receiver.rec.closeCh)
	}
	n.receivers = nil
}
