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

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestConflictBasics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const (
		numWorkers     = 8
		numSlots       = 4096
		workingSetSize = 4096
		batchSize      = 32
		totalBatches   = 10000
	)

	conflictArray := make([]int, workingSetSize)
	driver := newConflictTestDriver(
		numWorkers, numSlots, newUniformGenerator(workingSetSize, batchSize, numSlots),
	).WithExecFunc(
		func(txn *txnForTest) error {
			for _, key := range txn.ConflictKeys() {
				// Access a position in the array without synchronization,
				// so that if causality check is buggy, the Go race detection would fail.
				conflictArray[key]++
			}
			return nil
		})

	require.NoError(t, driver.Run(ctx, totalBatches))
	require.NoError(t, driver.Wait(ctx))
	driver.Close()
}

func BenchmarkLowConflicts(b *testing.B) {
	log.SetLevel(zapcore.WarnLevel)
	defer log.SetLevel(zapcore.InfoLevel)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const (
		numWorkers = 8
		numSlots   = 1024 * 1024
		// Expected conflict rate = 0.00006
		workingSetSize = 4096 * 4096
		batchSize      = 8
	)

	totalBatches := b.N / 8
	driver := newConflictTestDriver(
		numWorkers,
		numSlots,
		newUniformGenerator(workingSetSize, batchSize, numSlots))
	if err := driver.Run(ctx, totalBatches); err != nil {
		panic(err)
	}
	driver.Close()
}

func BenchmarkMediumConflicts(b *testing.B) {
	log.SetLevel(zapcore.WarnLevel)
	defer log.SetLevel(zapcore.InfoLevel)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const (
		numWorkers = 8
		numSlots   = 1024 * 1024
		// Expected conflict rate = 0.0155
		workingSetSize = 4096
		batchSize      = 8
	)

	totalBatches := b.N / 8
	driver := newConflictTestDriver(
		numWorkers,
		numSlots,
		newUniformGenerator(workingSetSize, batchSize, numSlots))
	if err := driver.Run(ctx, totalBatches); err != nil {
		panic(err)
	}
	driver.Close()
}

func BenchmarkHighConflicts(b *testing.B) {
	log.SetLevel(zapcore.WarnLevel)
	defer log.SetLevel(zapcore.InfoLevel)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const (
		numWorkers = 8
		numSlots   = 1024 * 1024
		// Expected conflict rate = 0.41
		workingSetSize = 128
		batchSize      = 8
	)

	totalBatches := b.N / 8
	driver := newConflictTestDriver(
		numWorkers,
		numSlots,
		newUniformGenerator(workingSetSize, batchSize, numSlots))
	if err := driver.Run(ctx, totalBatches); err != nil {
		panic(err)
	}
	driver.Close()
}
