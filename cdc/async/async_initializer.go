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
}

// NewInitializer creates a new initializer.
func NewInitializer() *Initializer {
	return &Initializer{
		initialized:  atomic.NewBool(false),
		initializing: atomic.NewBool(false),
		initError:    atomic.NewError(nil),
	}
}

// TryInitialize tries to initialize the module asynchronously.
// returns true if the module is already initialized or initialized successfully.
// returns false if the module is initializing or failed to initialize.
// returns error if the module failed to initialize.
// It will only initialize the module once.
// It's not thread-safe.
func (initializer *Initializer) TryInitialize(ctx context.Context,
	initFunc func(ctx context.Context) error,
	pool workerpool.AsyncPool,
) (bool, error) {
	if initializer.initialized.Load() {
		return true, nil
	}
	if initializer.initializing.CompareAndSwap(false, true) {
		initialCtx, cancelInitialize := context.WithCancel(ctx)
		initializer.initialWaitGroup.Add(1)
		initializer.cancelInitialize = cancelInitialize
		log.Info("submit async initializer task to the worker pool")
		err := pool.Go(initialCtx, func() {
			defer initializer.initialWaitGroup.Done()
			if err := initFunc(initialCtx); err != nil {
				initializer.initError.Store(errors.Trace(err))
			} else {
				initializer.initialized.Store(true)
			}
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
	return initializer.initialized.Load(), nil
}

// Terminate terminates the initializer.
// It will cancel the initialization if it is initializing. and wait for the initialization to finish.
// It's not thread-safe.
func (initializer *Initializer) Terminate() {
	if initializer.initializing.Load() {
		if initializer.cancelInitialize != nil {
			initializer.cancelInitialize()
		}
		initializer.initialWaitGroup.Wait()
	}
	initializer.initializing.Store(false)
	initializer.initialized.Store(false)
	initializer.initError.Store(nil)
}
