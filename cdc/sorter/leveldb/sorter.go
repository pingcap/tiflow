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

package leveldb

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sorter"
	"github.com/pingcap/tiflow/cdc/sorter/encoding"
	"github.com/pingcap/tiflow/cdc/sorter/leveldb/message"
	"github.com/pingcap/tiflow/pkg/actor"
	actormsg "github.com/pingcap/tiflow/pkg/actor/message"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	// Capacity of db sorter input and output channels.
	sorterInputCap, sorterOutputCap = 64, 64
	// Max size of received event batch.
	batchReceiveEventSize = 32
)

var levelDBSorterIDAlloc uint32 = 0

func allocID() uint32 {
	return atomic.AddUint32(&levelDBSorterIDAlloc, 1)
}

type common struct {
	dbActorID actor.ID
	dbRouter  *actor.Router

	uid     uint32
	tableID uint64
	serde   *encoding.MsgPackGenSerde
	errCh   chan error
}

// reportError notifies Sorter to return an error and close.
func (c *common) reportError(msg string, err error) {
	if errors.Cause(err) != context.Canceled {
		log.L().WithOptions(zap.AddCallerSkip(1)).
			Warn(msg, zap.Uint64("tableID", c.tableID), zap.Error(err))
	}
	select {
	case c.errCh <- err:
	default:
		// It means there is an error already.
	}
}

// Sorter accepts out-of-order raw kv entries and output sorted entries
type Sorter struct {
	common

	writerRouter  *actor.Router
	writerActorID actor.ID

	readerRouter  *actor.Router
	readerActorID actor.ID

	outputCh chan *model.PolymorphicEvent

	closed int32
}

// NewSorter creates a new Sorter
func NewSorter(
	ctx context.Context, tableID int64, startTs uint64,
	dbRouter *actor.Router, dbActorID actor.ID,
	writerSystem *actor.System, writerRouter *actor.Router,
	readerSystem *actor.System, readerRouter *actor.Router,
	compact *CompactScheduler, cfg *config.DBConfig,
) (*Sorter, error) {
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	metricIterDuration := sorterIterReadDurationHistogram.MustCurryWith(
		prometheus.Labels{"id": changefeedID})
	metricTotalEventsKV := sorter.EventCount.WithLabelValues(changefeedID, "kv")
	metricTotalEventsResolvedTs := sorter.EventCount.WithLabelValues(changefeedID, "resolved")

	// TODO: test capture the same table multiple times.
	uid := allocID()
	actorID := actor.ID(uid)
	c := common{
		dbActorID: dbActorID,
		dbRouter:  dbRouter,
		uid:       uid,
		tableID:   uint64(tableID),
		serde:     &encoding.MsgPackGenSerde{},
		errCh:     make(chan error, 1),
	}

	w := &writer{
		common:        c,
		readerRouter:  readerRouter,
		readerActorID: actorID,

		metricTotalEventsKV:         metricTotalEventsKV,
		metricTotalEventsResolvedTs: metricTotalEventsResolvedTs,
	}
	wmb := actor.NewMailbox(actorID, sorterInputCap)
	err := writerSystem.Spawn(wmb, w)
	if err != nil {
		return nil, errors.Trace(err)
	}

	outputCh := make(chan *model.PolymorphicEvent, sorterOutputCap)

	r := &reader{
		common: c,

		state: pollState{
			outputBuf: newOutputBuffer(batchReceiveEventSize),

			maxCommitTs:         uint64(0),
			maxResolvedTs:       uint64(0),
			exhaustedResolvedTs: uint64(0),

			readerID:     actorID,
			readerRouter: readerRouter,

			compactorID:           dbActorID,
			compact:               compact,
			iterMaxAliveDuration:  time.Duration(cfg.IteratorMaxAliveDuration) * time.Millisecond,
			iterFirstSlowDuration: time.Duration(cfg.IteratorSlowReadDuration) * time.Millisecond,

			metricIterFirst:   metricIterDuration.WithLabelValues("first"),
			metricIterRelease: metricIterDuration.WithLabelValues("release"),
		},
		delete: deleteThrottle{
			countThreshold: cfg.CompactionDeletionThreshold,
			period:         time.Duration(cfg.CompactionPeriod * int(time.Second)),
		},

		lastSentResolvedTs: startTs,
		outputCh:           outputCh,

		metricIterReadDuration: metricIterDuration.WithLabelValues("read"),
		metricIterNextDuration: metricIterDuration.WithLabelValues("next"),
	}
	rmb := actor.NewMailbox(actorID, sorterInputCap)
	err = readerSystem.Spawn(rmb, r)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Sorter{
		common:        c,
		writerRouter:  writerRouter,
		writerActorID: actorID,
		readerRouter:  readerRouter,
		readerActorID: actorID,
		outputCh:      outputCh,
	}, nil
}

// Run runs Sorter
func (ls *Sorter) Run(ctx context.Context) error {
	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case err = <-ls.errCh:
	}
	// TODO caller should pass context.
	deadline := time.Now().Add(1 * time.Second)
	ctx, cancel := context.WithDeadline(context.TODO(), deadline)
	defer cancel()
	atomic.StoreInt32(&ls.closed, 1)
	_ = ls.writerRouter.SendB(
		ctx, ls.writerActorID, actormsg.StopMessage())
	_ = ls.readerRouter.SendB(
		ctx, ls.readerActorID, actormsg.StopMessage())
	return errors.Trace(err)
}

// AddEntry adds an RawKVEntry to the EntryGroup
func (ls *Sorter) AddEntry(ctx context.Context, event *model.PolymorphicEvent) {
	if atomic.LoadInt32(&ls.closed) != 0 {
		return
	}
	msg := actormsg.SorterMessage(message.Task{
		UID:        ls.uid,
		TableID:    ls.tableID,
		InputEvent: event,
	})
	_ = ls.writerRouter.SendB(ctx, ls.writerActorID, msg)
}

// TryAddEntry tries to add an RawKVEntry to the EntryGroup
func (ls *Sorter) TryAddEntry(
	ctx context.Context, event *model.PolymorphicEvent,
) (bool, error) {
	if atomic.LoadInt32(&ls.closed) != 0 {
		return false, nil
	}
	msg := actormsg.SorterMessage(message.Task{
		UID:        ls.uid,
		TableID:    ls.tableID,
		InputEvent: event,
	})
	err := ls.writerRouter.Send(ls.writerActorID, msg)
	if err != nil {
		if cerror.ErrMailboxFull.Equal(err) {
			return false, nil
		}
		return false, errors.Trace(err)
	}
	return true, nil
}

// Output returns the sorted raw kv output channel
func (ls *Sorter) Output() <-chan *model.PolymorphicEvent {
	// Notify reader to read sorted events
	msg := actormsg.SorterMessage(message.Task{
		UID:     ls.uid,
		TableID: ls.tableID,
		ReadTs:  message.ReadTs{},
	})
	// It's ok to ignore error, as reader is either channel full or stopped.
	// If it's channel full, it has been notified by others, and caller will
	// receive new resolved events eventually.
	//
	// TODO: Consider if we are sending too many msgs here.
	//       It may waste CPU and be a bottleneck.
	_ = ls.readerRouter.Send(ls.readerActorID, msg)
	return ls.outputCh
}

// CleanupFunc returns a function that cleans up sorter's data.
func (ls *Sorter) CleanupFunc() func(context.Context) error {
	return func(ctx context.Context) error {
		task := message.Task{UID: ls.uid, TableID: ls.tableID}
		task.DeleteReq = &message.DeleteRequest{
			// We do not set task.Delete.Count, because we don't know
			// how many key-value pairs in the range.
			Range: [2][]byte{
				encoding.EncodeTsKey(ls.uid, ls.tableID, 0),
				encoding.EncodeTsKey(ls.uid, ls.tableID+1, 0),
			},
		}
		return ls.dbRouter.SendB(ctx, ls.dbActorID, actormsg.SorterMessage(task))
	}
}
