// Copyright 2023 PingCAP, Inc.
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

package sinkmanager

import (
	"context"
	"math"
	"testing"

	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine/memory"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

// MockPD only for test.
type MockPD struct {
	pd.Client
	ts int64
}

// GetTS implements the PD interface.
func (p *MockPD) GetTS(_ context.Context) (int64, int64, error) {
	if p.ts != 0 {
		return p.ts, p.ts, nil
	}
	return math.MaxInt64, math.MaxInt64, nil
}

// nolint:revive
// In test it is ok move the ctx to the second parameter.
func CreateManagerWithMemEngine(
	t *testing.T,
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	changefeedInfo *model.ChangeFeedInfo,
	errChan chan error,
) (*SinkManager, *sourcemanager.SourceManager, engine.SortEngine) {
	sortEngine := memory.New(context.Background())
	up := upstream.NewUpstream4Test(&MockPD{})
	sm := sourcemanager.NewForTest(changefeedID, up, &entry.MockMountGroup{},
		sortEngine, errChan, false)
	manager, err := New(
		ctx, changefeedID, changefeedInfo, up,
		&entry.MockSchemaStorage{Resolved: math.MaxUint64},
		nil, sm,
		errChan)
	require.NoError(t, err)
	return manager, sm, sortEngine
}
