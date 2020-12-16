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

package workerpool

import (
	"context"
	"time"
)

// WorkerPool runs a number of Goroutines that process the submitted events.
// Each EventHandle is bound to a fixed worker thread to prevent data races.
type WorkerPool interface {
	RegisterEvent(f func(ctx context.Context, event interface{}) error) EventHandle
	Run(ctx context.Context) error
}

// EventHandle is a handle for a registered event.
// Since events are executed asynchronously, errors should be collected from ErrCh().
type EventHandle interface {
	AddEvent(ctx context.Context, event interface{}) error
	SetTimer(interval time.Duration, f func(ctx context.Context) error)
	Unregister()
	ErrCh() <-chan error
}
