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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/sorter"
	"github.com/pingcap/tiflow/cdc/sorter/leveldb"
	"github.com/pingcap/tiflow/cdc/sorter/memory"
	"github.com/pingcap/tiflow/cdc/sorter/unified"
	"github.com/pingcap/tiflow/pkg/actor"
	"github.com/pingcap/tiflow/pkg/actor/message"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pipeline"
	pmessage "github.com/pingcap/tiflow/pkg/pipeline/message"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	flushMemoryMetricsDuration = time.Second * 5
)

type sorterNode struct {
	sorter sorter.EventSorter

	tableID   model.TableID
	tableName string // quoted schema and table, used in metircs only

	// for per-table flow control
	flowController tableFlowController

	mounter entry.Mounter

	eg     *errgroup.Group
	cancel context.CancelFunc

	// The latest resolved ts that sorter has received.
	resolvedTs model.Ts

	// The latest barrier ts that sorter has received.
	barrierTs model.Ts

	replConfig *config.ReplicaConfig

	// isTableActorMode identify if the sorter node is run is actor mode, todo: remove it after GA
	isTableActorMode bool
}

func newSorterNode(
	tableName string, tableID model.TableID, startTs model.Ts,
	flowController tableFlowController, mounter entry.Mounter,
	replConfig *config.ReplicaConfig,
) *sorterNode {
	return &sorterNode{
		tableName:      tableName,
		tableID:        tableID,
		flowController: flowController,
		mounter:        mounter,
		resolvedTs:     startTs,
		barrierTs:      startTs,
		replConfig:     replConfig,
	}
}

func (n *sorterNode) Init(ctx pipeline.NodeContext) error {
	wg := errgroup.Group{}
	return n.start(ctx, false, &wg, 0, nil)
}

func createSorter(ctx pipeline.NodeContext, tableName string, tableID model.TableID) (sorter.EventSorter, error) {
	sortEngine := ctx.ChangefeedVars().Info.Engine
	switch sortEngine {
	case model.SortInMemory:
		return memory.NewEntrySorter(), nil
	case model.SortUnified, model.SortInFile /* `file` becomes an alias of `unified` for backward compatibility */ :
		if sortEngine == model.SortInFile {
			log.Warn("File sorter is obsolete and replaced by unified sorter. Please revise your changefeed settings",
				zap.String("namesapce", ctx.ChangefeedVars().ID.Namespace),
				zap.String("changefeed", ctx.ChangefeedVars().ID.ID),
				zap.String("tableName", tableName))
		}

		if config.GetGlobalServerConfig().Debug.EnableDBSorter {
			startTs := ctx.ChangefeedVars().Info.StartTs
			ssystem := ctx.GlobalVars().SorterSystem
			dbActorID := ssystem.DBActorID(uint64(tableID))
			compactScheduler := ctx.GlobalVars().SorterSystem.CompactScheduler()
			levelSorter, err := leveldb.NewSorter(
				ctx, tableID, startTs, ssystem.DBRouter, dbActorID,
				ssystem.WriterSystem, ssystem.WriterRouter,
				ssystem.ReaderSystem, ssystem.ReaderRouter,
				compactScheduler, config.GetGlobalServerConfig().Debug.DB)
			if err != nil {
				return nil, err
			}
			return levelSorter, nil
		}
		// Sorter dir has been set and checked when server starts.
		// See https://github.com/pingcap/tiflow/blob/9dad09/cdc/server.go#L275
		sortDir := config.GetGlobalServerConfig().Sorter.SortDir
		unifiedSorter, err := unified.NewUnifiedSorter(sortDir, ctx.ChangefeedVars().ID, tableName, tableID)
		if err != nil {
			return nil, err
		}
		return unifiedSorter, nil
	default:
		return nil, cerror.ErrUnknownSortEngine.GenWithStackByArgs(sortEngine)
	}
}

func (n *sorterNode) start(
	ctx pipeline.NodeContext, isTableActorMode bool, eg *errgroup.Group,
	tableActorID actor.ID, tableActorRouter *actor.Router[pmessage.Message],
) error {
	n.isTableActorMode = isTableActorMode
	n.eg = eg
	stdCtx, cancel := context.WithCancel(ctx)
	n.cancel = cancel

	eventSorter, err := createSorter(ctx, n.tableName, n.tableID)
	if err != nil {
		return errors.Trace(err)
	}

	failpoint.Inject("ProcessorAddTableError", func() {
		failpoint.Return(errors.New("processor add table injected error"))
	})
	n.eg.Go(func() error {
		ctx.Throw(errors.Trace(eventSorter.Run(stdCtx)))
		return nil
	})
	n.eg.Go(func() error {
		lastSentResolvedTs := uint64(0)
		lastSendResolvedTsTime := time.Now() // the time at which we last sent a resolved-ts.
		lastCRTs := uint64(0)                // the commit-ts of the last row changed we sent.

		metricsTableMemoryHistogram := tableMemoryHistogram.
			WithLabelValues(ctx.ChangefeedVars().ID.Namespace, ctx.ChangefeedVars().ID.ID)
		metricsTicker := time.NewTicker(flushMemoryMetricsDuration)
		defer metricsTicker.Stop()

		for {
			// We must call `sorter.Output` before receiving resolved events.
			// Skip calling `sorter.Output` and caching output channel may fail
			// to receive any events.
			output := eventSorter.Output()
			select {
			case <-stdCtx.Done():
				return nil
			case <-metricsTicker.C:
				metricsTableMemoryHistogram.Observe(float64(n.flowController.GetConsumption()))
			case msg, ok := <-output:
				if !ok {
					// sorter output channel closed
					return nil
				}
				if msg == nil || msg.RawKV == nil {
					log.Panic("unexpected empty msg", zap.Reflect("msg", msg))
				}
				if msg.RawKV.OpType != model.OpTypeResolved {
					err := n.mounter.DecodeEvent(ctx, msg)
					if err != nil {
						return errors.Trace(err)
					}

					commitTs := msg.CRTs
					// We interpolate a resolved-ts if none has been sent for some time.
					if time.Since(lastSendResolvedTsTime) > resolvedTsInterpolateInterval {
						// checks the condition: cur_event_commit_ts > prev_event_commit_ts > last_resolved_ts
						// If this is true, it implies that (1) the last transaction has finished, and we are processing
						// the first event in a new transaction, (2) a resolved-ts is safe to be sent, but it has not yet.
						// This means that we can interpolate prev_event_commit_ts as a resolved-ts, improving the frequency
						// at which the sink flushes.
						if lastCRTs > lastSentResolvedTs && commitTs > lastCRTs {
							lastSentResolvedTs = lastCRTs
							lastSendResolvedTsTime = time.Now()
							msg := model.NewResolvedPolymorphicEvent(0, lastCRTs)
							ctx.SendToNextNode(pmessage.PolymorphicEventMessage(msg))
						}
					}

					// We calculate memory consumption by RowChangedEvent size.
					// It's much larger than RawKVEntry.
					size := uint64(msg.Row.ApproximateBytes())
					// NOTE we allow the quota to be exceeded if blocking means interrupting a transaction.
					// Otherwise the pipeline would deadlock.
					err = n.flowController.Consume(msg, size, func(batch bool) error {
						if batch {
							log.Panic("cdc does not support the batch resolve mechanism at this time")
						} else if lastCRTs > lastSentResolvedTs {
							// If we are blocking, we send a Resolved Event here to elicit a sink-flush.
							// Not sending a Resolved Event here will very likely deadlock the pipeline.
							lastSentResolvedTs = lastCRTs
							lastSendResolvedTsTime = time.Now()
							msg := model.NewResolvedPolymorphicEvent(0, lastCRTs)
							ctx.SendToNextNode(pmessage.PolymorphicEventMessage(msg))
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
					if isTableActorMode {
						msg := message.ValueMessage(pmessage.TickMessage())
						_ = tableActorRouter.Send(tableActorID, msg)
					}
					lastSentResolvedTs = msg.CRTs
					lastSendResolvedTsTime = time.Now()
				}
				ctx.SendToNextNode(pmessage.PolymorphicEventMessage(msg))
			}
		}
	})
	n.sorter = eventSorter
	return nil
}

// Receive receives the message from the previous node
func (n *sorterNode) Receive(ctx pipeline.NodeContext) error {
	_, err := n.TryHandleDataMessage(ctx, ctx.Message())
	return err
}

// handleRawEvent process the raw kv event,send it to sorter
func (n *sorterNode) handleRawEvent(ctx context.Context, event *model.PolymorphicEvent) {
	rawKV := event.RawKV
	if rawKV != nil && rawKV.OpType == model.OpTypeResolved {
		// Puller resolved ts should not fall back.
		resolvedTs := rawKV.CRTs
		oldResolvedTs := atomic.SwapUint64(&n.resolvedTs, resolvedTs)
		if oldResolvedTs > resolvedTs {
			log.Panic("resolved ts regression",
				zap.Int64("tableID", n.tableID),
				zap.Uint64("resolvedTs", resolvedTs),
				zap.Uint64("oldResolvedTs", oldResolvedTs))
		}
		atomic.StoreUint64(&n.resolvedTs, rawKV.CRTs)

		if resolvedTs > n.BarrierTs() &&
			!redo.IsConsistentEnabled(n.replConfig.Consistent.Level) {
			// Do not send resolved ts events that is larger than
			// barrier ts.
			// When DDL puller stall, resolved events that outputted by
			// sorter may pile up in memory, as they have to wait DDL.
			//
			// Disabled if redolog is on, it requires sink reports
			// resolved ts, conflicts to this change.
			// TODO: Remove redolog check once redolog decouples for global
			//       resolved ts.
			event = model.NewResolvedPolymorphicEvent(0, n.BarrierTs())
		}
	}
	n.sorter.AddEntry(ctx, event)
}

func (n *sorterNode) TryHandleDataMessage(
	ctx context.Context, msg pmessage.Message,
) (bool, error) {
	switch msg.Tp {
	case pmessage.MessageTypePolymorphicEvent:
		n.handleRawEvent(ctx, msg.PolymorphicEvent)
		return true, nil
	case pmessage.MessageTypeBarrier:
		n.updateBarrierTs(msg.BarrierTs)
		fallthrough
	default:
		ctx.(pipeline.NodeContext).SendToNextNode(msg)
		return true, nil
	}
}

func (n *sorterNode) updateBarrierTs(barrierTs model.Ts) {
	if barrierTs > n.BarrierTs() {
		atomic.StoreUint64(&n.barrierTs, barrierTs)
	}
}

func (n *sorterNode) releaseResource(changefeedID model.ChangeFeedID) {
	defer tableMemoryHistogram.DeleteLabelValues(changefeedID.Namespace, changefeedID.ID)
	// Since the flowController is implemented by `Cond`, it is not cancelable by a context
	// the flowController will be blocked in a background goroutine,
	// We need to abort the flowController manually in the nodeRunner
	n.flowController.Abort()
}

func (n *sorterNode) Destroy(ctx pipeline.NodeContext) error {
	n.cancel()
	n.releaseResource(ctx.ChangefeedVars().ID)
	return n.eg.Wait()
}

func (n *sorterNode) ResolvedTs() model.Ts {
	return atomic.LoadUint64(&n.resolvedTs)
}

// BarrierTs returns the sorter barrierTs
func (n *sorterNode) BarrierTs() model.Ts {
	return atomic.LoadUint64(&n.barrierTs)
}
