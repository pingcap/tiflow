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

	"github.com/stretchr/testify/require"
)

func TestConflict(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const (
		numWorkers     = 8
		numSlots       = 4096
		workingSetSize = 4096
		batchSize      = 32
		totalBatches   = 10000
	)
	driver := newConflictTestDriver(numWorkers, numSlots, newUniformGenerator(workingSetSize, batchSize))

	require.NoError(t, driver.Run(ctx, totalBatches))
	require.NoError(t, driver.Wait(ctx))
	driver.Close()
}

func BenchmarkLowConflicts(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const (
		numWorkers     = 8
		numSlots       = 1024 * 1024
		workingSetSize = 4096 * 4096
		batchSize      = 8
	)

	totalBatches := b.N / 8
	driver := newConflictTestDriver(numWorkers, numSlots, newUniformGenerator(workingSetSize, batchSize))
	if err := driver.Run(ctx, totalBatches); err != nil {
		panic(err)
	}
	driver.Close()
}

func BenchmarkHighConflicts(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const (
		numWorkers     = 8
		numSlots       = 1024 * 1024
		workingSetSize = 4096
		batchSize      = 8
	)

	totalBatches := b.N / 8
	driver := newConflictTestDriver(numWorkers, numSlots, newUniformGenerator(workingSetSize, batchSize))
	if err := driver.Run(ctx, totalBatches); err != nil {
		panic(err)
	}
	driver.Close()
}
