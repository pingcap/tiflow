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

	"github.com/pingcap/log"
	"go.uber.org/zap"

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
	sortEngine model.SortEngine
	sortDir    string
	sorter     puller.EventSorter

	changeFeedID model.ChangeFeedID
	tableID      model.TableID
	tableName    string // quoted schema and table, used in metircs only

	flowController tableFlowController

	wg     errgroup.Group
	cancel context.CancelFunc
}

func newSorterNode(
	sortEngine model.SortEngine,
	sortDir string,
	changeFeedID model.ChangeFeedID,
	tableName string, tableID model.TableID,
	flowController tableFlowController) pipeline.Node {
	return &sorterNode{
		sortEngine: sortEngine,
		sortDir:    sortDir,

		changeFeedID: changeFeedID,
		tableID:      tableID,
		tableName:    tableName,

		flowController: flowController,
	}
}

func (n *sorterNode) Init(ctx pipeline.NodeContext) error {
	stdCtx, cancel := context.WithCancel(ctx.StdContext())
	n.cancel = cancel
	var sorter puller.EventSorter
	switch n.sortEngine {
	case model.SortInMemory:
		sorter = puller.NewEntrySorter()
	case model.SortInFile:
		err := util.IsDirAndWritable(n.sortDir)
		if err != nil {
			if os.IsNotExist(errors.Cause(err)) {
				err = os.MkdirAll(n.sortDir, 0o755)
				if err != nil {
					return errors.Annotate(cerror.WrapError(cerror.ErrProcessorSortDir, err), "create dir")
				}
			} else {
				return errors.Annotate(cerror.WrapError(cerror.ErrProcessorSortDir, err), "sort dir check")
			}
		}

		sorter = puller.NewFileSorter(n.sortDir)
	case model.SortUnified:
		err := psorter.UnifiedSorterCheckDir(n.sortDir)
		if err != nil {
			return errors.Trace(err)
		}
		sorter = psorter.NewUnifiedSorter(n.sortDir, n.changeFeedID, n.tableName, n.tableID, ctx.Vars().CaptureAddr)
	default:
		return cerror.ErrUnknownSortEngine.GenWithStackByArgs(n.sortEngine)
	}
	failpoint.Inject("ProcessorAddTableError", func() {
		failpoint.Return(errors.New("processor add table injected error"))
	})
	n.wg.Go(func() error {
		ctx.Throw(errors.Trace(sorter.Run(stdCtx)))
		return nil
	})
	n.wg.Go(func() error {
		lastSentResolvedTs := uint64(0)
		lastCRTs := uint64(0)
		for {
			select {
			case <-stdCtx.Done():
				return nil
			case msg := <-sorter.Output():
				if msg == nil {
					continue
				}
				if msg.RawKV != nil && msg.RawKV.OpType != model.OpTypeResolved {
					size := uint64(msg.RawKV.ApproximateSize() * 2)
					commitTs := msg.CRTs
					err := n.flowController.Consume(commitTs, size, func() error {
						if lastCRTs > lastSentResolvedTs {
							lastSentResolvedTs = lastCRTs
							ctx.SendToNextNode(pipeline.PolymorphicEventMessage(model.NewResolvedPolymorphicEvent(0, lastCRTs)))
						}
						return nil
					})
					if err != nil {
						if cerror.ErrFlowControllerAborted.Equal(err) {
							log.Info("flow control cancelled for table",
								zap.Int64("tableID", n.tableID),
								zap.String("tableName", n.tableName))
						} else {
							ctx.Throw(err)
						}
						return nil
					}
					lastCRTs = commitTs
				}

				if msg.RawKV != nil && msg.RawKV.OpType == model.OpTypeResolved {
					if msg.CRTs < lastSentResolvedTs {
						continue
					}
					lastSentResolvedTs = msg.CRTs
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
		n.sorter.AddEntry(ctx.StdContext(), msg.PolymorphicEvent)
	default:
		ctx.SendToNextNode(msg)
	}
	return nil
}

func (n *sorterNode) Destroy(ctx pipeline.NodeContext) error {
	n.cancel()
	return n.wg.Wait()
}
