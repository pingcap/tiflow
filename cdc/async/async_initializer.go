// Copyright 2024 PingCAP, Inc.
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

package async

import (
	"context"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/workerpool"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Initializer is a helper struct to initialize a changefeed asynchronously.
type Initializer struct {
	// state related fields
	initialized  *atomic.Bool
	initializing *atomic.Bool
	initError    *atomic.Error

	// func to cancel the changefeed initialization
	cancelInitialize context.CancelFunc
	initialWaitGroup sync.WaitGroup

	initFunc func(ctx context.Context) error
}

func NewInitializer(initFunc func(ctx context.Context) error) *Initializer {
	return &Initializer{
		initialized:  atomic.NewBool(false),
		initializing: atomic.NewBool(false),
		initError:    atomic.NewError(nil),
		initFunc:     initFunc,
	}
}

// TryInitialize tries to initialize the changefeed asynchronously.
func (initializer *Initializer) TryInitialize(ctx context.Context, pool workerpool.AsyncPool) (bool, error) {
	if initializer.initialized.Load() {
		return true, nil
	}
	if !initializer.initializing.Load() {
		initializer.initializing.Store(true)
		initialCtx, cancelInitialize := context.WithCancel(ctx)
		initializer.initialWaitGroup.Add(1)
		initializer.cancelInitialize = cancelInitialize
		err := pool.Go(initialCtx, func() {
			defer initializer.initialWaitGroup.Done()
			if err := initializer.initFunc(initialCtx); err != nil {
				initializer.initError.Store(errors.Trace(err))
			}
			initializer.initializing.Store(false)
		})
		if err != nil {
			log.Error("failed to submit async initializer task to the worker pool", zap.Error(err))
			initializer.initialWaitGroup.Done()
			return false, errors.Trace(err)
		}
	}
	if initializer.initError.Load() != nil {
		return false, errors.Trace(initializer.initError.Load())
	}
	return !initializer.initializing.Load(), nil
}

// Terminate terminates the initializer.
func (initializer *Initializer) Terminate() {
	if initializer.initializing.Load() {
		if initializer.cancelInitialize != nil {
			initializer.cancelInitialize()
		}
		initializer.initialWaitGroup.Wait()
	}
}
