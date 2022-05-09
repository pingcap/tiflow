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

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/sorter"
	"github.com/pingcap/tiflow/cdc/sorter/memory"
	"github.com/pingcap/tiflow/cdc/sorter/unified"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	"github.com/pingcap/tiflow/pkg/pipeline"
	pmessage "github.com/pingcap/tiflow/pkg/pipeline/message"
	"github.com/stretchr/testify/require"
)

func TestUnifiedSorterFileLockConflict(t *testing.T) {
	dir := t.TempDir()

	// GlobalServerConfig overrides dir parameter in NewUnifiedSorter.
	config.GetGlobalServerConfig().Sorter.SortDir = dir
	config.GetGlobalServerConfig().Debug.EnableDBSorter = false
	defer func() {
		// Restore default server config.
		config.GetGlobalServerConfig().Debug.EnableDBSorter = true
	}()

	_, err := unified.NewUnifiedSorter(dir,
		model.DefaultChangeFeedID("changefeed-id-test"),
		"test", 0)
	require.Nil(t, err)

	unified.ResetGlobalPoolWithoutCleanup()
	ctx := cdcContext.NewBackendContext4Test(true)
	ctx.ChangefeedVars().Info.Engine = model.SortUnified
	ctx.ChangefeedVars().Info.SortDir = dir
	sorter := sorterNode{}
	err = sorter.Init(pipeline.MockNodeContext4Test(ctx, pmessage.Message{}, nil))
	require.True(t, strings.Contains(err.Error(), "file lock conflict"))
}

func TestSorterResolvedTs(t *testing.T) {
	t.Parallel()
	sn := newSorterNode("tableName", 1, 1, nil, nil, &config.ReplicaConfig{
		Consistent: &config.ConsistentConfig{},
	})
	sn.sorter = memory.NewEntrySorter()
	require.EqualValues(t, 1, sn.ResolvedTs())
	nctx := pipeline.NewNodeContext(
		cdcContext.NewContext(context.Background(), nil),
		pmessage.PolymorphicEventMessage(model.NewResolvedPolymorphicEvent(0, 2)),
		nil,
	)
	err := sn.Receive(nctx)
	require.Nil(t, err)
	require.EqualValues(t, 2, sn.ResolvedTs())
}

type checkSorter struct {
	ch chan *model.PolymorphicEvent
}

var _ sorter.EventSorter = (*checkSorter)(nil)

func (c *checkSorter) Run(ctx context.Context) error {
	return nil
}

func (c *checkSorter) AddEntry(ctx context.Context, entry *model.PolymorphicEvent) {
	c.ch <- entry
}

func (c *checkSorter) TryAddEntry(
	ctx context.Context, entry *model.PolymorphicEvent,
) (bool, error) {
	select {
	case c.ch <- entry:
		return true, nil
	default:
		return false, nil
	}
}

func (c *checkSorter) Output() <-chan *model.PolymorphicEvent {
	return c.ch
}

func TestSorterResolvedTsLessEqualBarrierTs(t *testing.T) {
	t.Parallel()
	sch := make(chan *model.PolymorphicEvent, 1)
	s := &checkSorter{ch: sch}
	sn := newSorterNode("tableName", 1, 1, nil, nil, &config.ReplicaConfig{
		Consistent: &config.ConsistentConfig{},
	})
	sn.sorter = s

	ch := make(chan pmessage.Message, 1)
	require.EqualValues(t, 1, sn.ResolvedTs())

	// Resolved ts must not regress even if there is no barrier ts message.
	resolvedTs1 := pmessage.PolymorphicEventMessage(model.NewResolvedPolymorphicEvent(0, 1))
	nctx := pipeline.NewNodeContext(
		cdcContext.NewContext(context.Background(), nil), resolvedTs1, ch)
	err := sn.Receive(nctx)
	require.Nil(t, err)
	require.EqualValues(t, model.NewResolvedPolymorphicEvent(0, 1), <-sch)

	// Advance barrier ts.
	nctx = pipeline.NewNodeContext(
		cdcContext.NewContext(context.Background(), nil),
		pmessage.BarrierMessage(2),
		ch,
	)
	err = sn.Receive(nctx)
	require.Nil(t, err)
	require.EqualValues(t, 2, sn.BarrierTs())
	// Barrier message must be passed to the next node.
	require.EqualValues(t, pmessage.BarrierMessage(2), <-ch)

	resolvedTs2 := pmessage.PolymorphicEventMessage(model.NewResolvedPolymorphicEvent(0, 2))
	nctx = pipeline.NewNodeContext(
		cdcContext.NewContext(context.Background(), nil), resolvedTs2, nil)
	err = sn.Receive(nctx)
	require.Nil(t, err)
	require.EqualValues(t, resolvedTs2.PolymorphicEvent, <-s.Output())

	resolvedTs3 := pmessage.PolymorphicEventMessage(model.NewResolvedPolymorphicEvent(0, 3))
	nctx = pipeline.NewNodeContext(
		cdcContext.NewContext(context.Background(), nil), resolvedTs3, nil)
	err = sn.Receive(nctx)
	require.Nil(t, err)
	require.EqualValues(t, resolvedTs2.PolymorphicEvent, <-s.Output())

	resolvedTs4 := pmessage.PolymorphicEventMessage(model.NewResolvedPolymorphicEvent(0, 4))
	sn.replConfig.Consistent.Level = string(redo.ConsistentLevelEventual)
	nctx = pipeline.NewNodeContext(
		cdcContext.NewContext(context.Background(), nil), resolvedTs4, nil)
	err = sn.Receive(nctx)
	require.Nil(t, err)
	resolvedTs4 = pmessage.PolymorphicEventMessage(model.NewResolvedPolymorphicEvent(0, 4))
	require.EqualValues(t, resolvedTs4.PolymorphicEvent, <-s.Output())
}

func TestSorterUpdateBarrierTs(t *testing.T) {
	t.Parallel()
	s := &sorterNode{barrierTs: 1}
	s.updateBarrierTs(model.Ts(2))
	require.Equal(t, model.Ts(2), s.BarrierTs())
	s.updateBarrierTs(model.Ts(1))
	require.Equal(t, model.Ts(2), s.BarrierTs())
}
