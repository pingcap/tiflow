// Copyright 2021 PingCAP, Inc.
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

package pipeline

import (
	"context"
	"strings"
	"testing"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/cdc/puller/sorter"
	"github.com/pingcap/ticdc/pkg/config"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	"github.com/pingcap/ticdc/pkg/leakutil"
	"github.com/pingcap/ticdc/pkg/pipeline"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	leakutil.SetUpLeakTest(
		m,
		goleak.IgnoreTopFunction("github.com/pingcap/ticdc/cdc/puller/sorter.newBackEndPool.func1"),
	)
}

func TestUnifiedSorterFileLockConflict(t *testing.T) {
	dir := t.TempDir()
	captureAddr := "0.0.0.0:0"

	// GlobalServerConfig overrides dir parameter in NewUnifiedSorter.
	config.GetGlobalServerConfig().Sorter.SortDir = dir
	_, err := sorter.NewUnifiedSorter(dir, "test-cf", "test", 0, captureAddr)
	require.Nil(t, err)

	sorter.ResetGlobalPoolWithoutCleanup()
	ctx := cdcContext.NewBackendContext4Test(true)
	ctx.ChangefeedVars().Info.Engine = model.SortUnified
	ctx.ChangefeedVars().Info.SortDir = dir
	sorter := sorterNode{}
	err = sorter.Init(pipeline.MockNodeContext4Test(ctx, pipeline.Message{}, nil))
	require.True(t, strings.Contains(err.Error(), "file lock conflict"))
}

func TestSorterResolvedTs(t *testing.T) {
	t.Parallel()
	sn := newSorterNode("tableName", 1, 1, nil, nil)
	sn.sorter = puller.NewEntrySorter()
	require.EqualValues(t, 1, sn.ResolvedTs())
	nctx := pipeline.NewNodeContext(
		cdcContext.NewContext(context.Background(), nil),
		pipeline.PolymorphicEventMessage(model.NewResolvedPolymorphicEvent(0, 2)),
		nil,
	)
	err := sn.Receive(nctx)
	require.Nil(t, err)
	require.EqualValues(t, 2, sn.ResolvedTs())
}
