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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	psorter "github.com/pingcap/ticdc/cdc/puller/sorter"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pipeline"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	flushMemoryMetricsDuration = time.Second * 5
)

type sorterNode struct {
	sortEngine model.SortEngine
	sortDir    string
	sorter     puller.EventSorter

	changeFeedID model.ChangeFeedID
	tableID      model.TableID
	tableName    string // quoted schema and table, used in metircs only

	// for per-table flow control
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
		sorter, err = psorter.NewUnifiedSorter(n.sortDir, n.changeFeedID, n.tableName, n.tableID, ctx.Vars().CaptureAddr)
		if err != nil {
			return errors.Trace(err)
		}
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

		metricsTableMemoryGauge := tableMemoryGauge.WithLabelValues(n.changeFeedID, ctx.Vars().CaptureAddr, n.tableName)
		metricsTicker := time.NewTicker(flushMemoryMetricsDuration)
		defer metricsTicker.Stop()

		for {
			select {
			case <-stdCtx.Done():
				return nil
			case <-metricsTicker.C:
				metricsTableMemoryGauge.Set(float64(n.flowController.GetConsumption()))
			case msg, ok := <-sorter.Output():
				if !ok {
					// sorter output channel closed
					return nil
				}
				if msg == nil || msg.RawKV == nil {
					log.Panic("unexpected empty msg", zap.Reflect("msg", msg))
				}
				if msg.RawKV.OpType != model.OpTypeResolved {
					size := uint64(msg.RawKV.ApproximateSize())
					commitTs := msg.CRTs
					// We interpolate a resolved-ts if none has been sent for some time.
					if time.Since(lastSendResolvedTsTime) > resolvedTsInterpolateInterval {
						// checks the condition: cur_event_commit_ts > prev_event_commit_ts > last_resolved_ts
						// If this is true, it implies that (1) the last transaction has finished, and we are processing
						// the first event in a new transaction, (2) a resolved-ts prev_event_commit_ts is safe to be sent,
						// but it has not yet.
						// This means that we can interpolate prev_event_commit_ts as a resolved-ts, improving the frequency
						// at which the sink flushes.
						if lastCRTs > lastSentResolvedTs && commitTs > lastCRTs {
							lastSentResolvedTs = lastCRTs
							lastSendResolvedTsTime = time.Now()
							ctx.SendToNextNode(pipeline.PolymorphicEventMessage(model.NewResolvedPolymorphicEvent(0, lastCRTs)))
						}
					}
					// NOTE we allow the quota to be exceeded if blocking means interrupting a transaction.
					// Otherwise the pipeline would deadlock.
					err := n.flowController.Consume(commitTs, size, func() error {
						if lastCRTs > lastSentResolvedTs {
							// If we are blocking, we send a Resolved Event here to elicit a sink-flush.
							// Not sending a Resolved Event here will very likely deadlock the pipeline.
							lastSentResolvedTs = lastCRTs
							lastSendResolvedTsTime = time.Now()
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
				} else {
					// handle OpTypeResolved
					if msg.CRTs < lastSentResolvedTs {
						continue
					}
					lastSentResolvedTs = msg.CRTs
					lastSendResolvedTsTime = time.Now()
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
	defer tableMemoryGauge.DeleteLabelValues(n.changeFeedID, ctx.Vars().CaptureAddr, n.tableName)
	n.cancel()
	return n.wg.Wait()
}
