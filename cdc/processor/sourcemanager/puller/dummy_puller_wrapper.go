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

package puller

import (
	"context"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/sorter"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/puller"
	"github.com/pingcap/tiflow/pkg/upstream"
)

var _ Wrapper = &dummyPullerWrapper{}

type dummyPullerWrapper struct{}

// NewPullerWrapperForTest creates a new puller wrapper for test.
func NewPullerWrapperForTest(
	changefeed model.ChangeFeedID,
	span tablepb.Span,
	tableName string,
	startTs model.Ts,
	bdrMode bool,
) Wrapper {
	return &dummyPullerWrapper{}
}

func (d *dummyPullerWrapper) Start(ctx context.Context, up *upstream.Upstream,
	eventSortEngine sorter.SortEngine, errCh chan<- error) {
}

func (d *dummyPullerWrapper) GetStats() puller.Stats {
	return puller.Stats{}
}

func (d *dummyPullerWrapper) Close() {}
