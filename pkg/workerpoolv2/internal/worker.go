// Copyright 2022 PingCAP, Inc.
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

package internal

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
)

type worker struct {
	notifyCh chan Poller
	pollers  sync.Map
	toRemove sync.Map
}

func newWorker() *worker {
	return &worker{
		notifyCh: make(chan Poller, 1024),
	}
}

func (w *worker) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case p := <-w.notifyCh:
			for w.doPoll(p) {
			}
		case <-ticker.C:
			w.pollers.Range(func(key, _ any) bool {
				p := key.(Poller)
				for w.doPoll(p) {
				}
				return true
			})
		}
	}
}

func (w *worker) doPoll(p Poller) bool {
	hasNext := p.Poll()

	if _, ok := w.toRemove.LoadAndDelete(p); ok {
		w.pollers.Delete(p)
		return false
	}
	return hasNext
}

func (w *worker) notify(p Poller, shouldExit bool) {
	if shouldExit {
		w.toRemove.Store(p, struct{}{})
	}

	select {
	case w.notifyCh <- p:
	default:
	}
}

func (w *worker) AddPoller(p Poller) {
	p.SetNotifyFunc(w.notify)
	w.pollers.Store(p, struct{}{})
}
