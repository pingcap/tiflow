// Copyright 2020 PingCAP, Inc.
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
	"os"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	psorter "github.com/pingcap/ticdc/cdc/puller/sorter"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pipeline"
	"github.com/pingcap/ticdc/pkg/util"
	"golang.org/x/sync/errgroup"
)

type sorterNode struct {
	sorter puller.EventSorter

	tableID   model.TableID
	tableName string // quoted schema and table, used in metircs only

	wg     errgroup.Group
	cancel context.CancelFunc
}

<<<<<<< HEAD
func newSorterNode(
	sortEngine model.SortEngine,
	sortDir string,
	changeFeedID model.ChangeFeedID,
	tableName string, tableID model.TableID) pipeline.Node {
	return &sorterNode{
		sortEngine: sortEngine,
		sortDir:    sortDir,

		changeFeedID: changeFeedID,
		tableID:      tableID,
		tableName:    tableName,
=======
func newSorterNode(tableName string, tableID model.TableID, flowController tableFlowController) pipeline.Node {
	return &sorterNode{
		tableName:      tableName,
		tableID:        tableID,
		flowController: flowController,
>>>>>>> 9b50616f (*: refine the vars in context.Context (#1459))
	}
}

func (n *sorterNode) Init(ctx pipeline.NodeContext) error {
	stdCtx, cancel := context.WithCancel(ctx)
	n.cancel = cancel
	var sorter puller.EventSorter
	sortEngine := ctx.ChangefeedVars().Info.Engine
	switch sortEngine {
	case model.SortInMemory:
		sorter = puller.NewEntrySorter()
	case model.SortInFile:
		sortDir := ctx.ChangefeedVars().Info.SortDir
		err := util.IsDirAndWritable(sortDir)
		if err != nil {
			if os.IsNotExist(errors.Cause(err)) {
				err = os.MkdirAll(sortDir, 0o755)
				if err != nil {
					return errors.Annotate(cerror.WrapError(cerror.ErrProcessorSortDir, err), "create dir")
				}
			} else {
				return errors.Annotate(cerror.WrapError(cerror.ErrProcessorSortDir, err), "sort dir check")
			}
		}

		sorter = puller.NewFileSorter(sortDir)
	case model.SortUnified:
		sortDir := ctx.ChangefeedVars().Info.SortDir
		err := psorter.UnifiedSorterCheckDir(sortDir)
		if err != nil {
			return errors.Trace(err)
		}
<<<<<<< HEAD
		sorter = psorter.NewUnifiedSorter(n.sortDir, n.changeFeedID, n.tableName, n.tableID, ctx.Vars().CaptureAddr)
=======
		sorter, err = psorter.NewUnifiedSorter(sortDir, ctx.ChangefeedVars().ID, n.tableName, n.tableID, ctx.GlobalVars().CaptureInfo.AdvertiseAddr)
		if err != nil {
			return errors.Trace(err)
		}
>>>>>>> 9b50616f (*: refine the vars in context.Context (#1459))
	default:
		return cerror.ErrUnknownSortEngine.GenWithStackByArgs(sortEngine)
	}
	failpoint.Inject("ProcessorAddTableError", func() {
		failpoint.Return(errors.New("processor add table injected error"))
	})
	n.wg.Go(func() error {
		ctx.Throw(errors.Trace(sorter.Run(stdCtx)))
		return nil
	})
	n.wg.Go(func() error {
<<<<<<< HEAD
=======
		// Since the flowController is implemented by `Cond`, it is not cancelable
		// by a context. We need to listen on cancellation and aborts the flowController
		// manually.
		<-stdCtx.Done()
		n.flowController.Abort()
		return nil
	})
	n.wg.Go(func() error {
		lastSentResolvedTs := uint64(0)
		lastSendResolvedTsTime := time.Now() // the time at which we last sent a resolved-ts.
		lastCRTs := uint64(0)                // the commit-ts of the last row changed we sent.

		metricsTableMemoryGauge := tableMemoryGauge.WithLabelValues(ctx.ChangefeedVars().ID, ctx.GlobalVars().CaptureInfo.AdvertiseAddr, n.tableName)
		metricsTicker := time.NewTicker(flushMemoryMetricsDuration)
		defer metricsTicker.Stop()

>>>>>>> 9b50616f (*: refine the vars in context.Context (#1459))
		for {
			select {
			case <-stdCtx.Done():
				return nil
			case msg := <-sorter.Output():
				if msg == nil {
					continue
				}
				ctx.SendToNextNode(pipeline.PolymorphicEventMessage(msg))
			}
		}
	})
	n.sorter = sorter
	return nil
}

// Receive receives the message from the previous node
func (n *sorterNode) Receive(ctx pipeline.NodeContext) error {
	msg := ctx.Message()
	switch msg.Tp {
	case pipeline.MessageTypePolymorphicEvent:
		n.sorter.AddEntry(ctx, msg.PolymorphicEvent)
	default:
		ctx.SendToNextNode(msg)
	}
	return nil
}

func (n *sorterNode) Destroy(ctx pipeline.NodeContext) error {
<<<<<<< HEAD
=======
	defer tableMemoryGauge.DeleteLabelValues(ctx.ChangefeedVars().ID, ctx.GlobalVars().CaptureInfo.AdvertiseAddr, n.tableName)
>>>>>>> 9b50616f (*: refine the vars in context.Context (#1459))
	n.cancel()
	return n.wg.Wait()
}
