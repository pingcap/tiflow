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

package checker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type slowIncrementer struct {
	atomic.Int64
}

func (i *slowIncrementer) Inc() int64 {
	time.Sleep(100 * time.Millisecond)
	return i.Add(1)
}

type job struct {
	inc *slowIncrementer
}

type baseAdder struct {
	base   int64
	closed atomic.Bool
}

func (a *baseAdder) add(_ context.Context, j job) (int64, error) {
	i := j.inc.Inc()
	return a.base + i, nil
}

func TestExampleWorkerPool(t *testing.T) {
	sum := int64(0)
	concurrency := 100
	jobNum := 1000
	incrementer := slowIncrementer{}

	pool := NewWorkerPool[job, int64](func(result int64) {
		sum += result
	})
	for i := 0; i < concurrency; i++ {
		worker := &baseAdder{base: 666}
		pool.Go(worker.add)
	}
	for i := 0; i < jobNum; i++ {
		pool.PutJob(job{inc: &incrementer})
	}

	err := pool.Wait()
	require.NoError(t, err)
	// sum 1 to 1000 = 500500
	require.Equal(t, int64(666*jobNum+500500), sum)
}

var (
	errMock = errors.New("mock error")
	errorAt = int64(500)
)

func (a *baseAdder) addAndError(_ context.Context, j job) (int64, error) {
	i := j.inc.Inc()
	if i == errorAt {
		if a.closed.Load() {
			panic("worker is used after closed")
		}
		a.closed.Store(true)
		return 0, errMock
	}
	return a.base + i, nil
}

func TestExampleWorkerPoolError(t *testing.T) {
	sum := int64(0)
	concurrency := 100
	jobNum := 1000
	incrementer := slowIncrementer{}

	pool := NewWorkerPool[job, int64](func(result int64) {
		sum += result
	})
	for i := 0; i < concurrency; i++ {
		worker := &baseAdder{base: 666}
		pool.Go(worker.addAndError)
	}
	for i := 0; i < jobNum; i++ {
		ok := pool.PutJob(job{inc: &incrementer})
		if !ok {
			require.GreaterOrEqual(t, int64(i), errorAt)
			break
		}
	}

	err := pool.Wait()
	require.ErrorIs(t, err, errMock)
}
