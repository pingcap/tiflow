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
		numSlots       = 128
		workingSetSize = 1024
		batchSize      = 8
		totalRows      = 1000
	)
	driver := newConflictTestDriver(numWorkers, numSlots, newUniformGenerator(workingSetSize, batchSize))

	require.NoError(t, driver.Run(ctx, totalRows))
	require.NoError(t, driver.Wait(ctx))
	driver.Close()
}
