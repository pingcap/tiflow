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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sorter"
	"github.com/pingcap/tiflow/cdc/sorter/encoding"
	"github.com/pingcap/tiflow/cdc/sorter/leveldb/message"
	"github.com/pingcap/tiflow/pkg/actor"
	actormsg "github.com/pingcap/tiflow/pkg/actor/message"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
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
	dbRouter  *actor.Router[message.Task]

	uid      uint32
	tableID  uint64
	serde    *encoding.MsgPackGenSerde
	errCh    chan error
	closedWg *sync.WaitGroup
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

	writerRouter  *actor.Router[message.Task]
	writerActorID actor.ID

	readerRouter  *actor.Router[message.Task]
	ReaderActorID actor.ID

	outputCh chan *model.PolymorphicEvent

	closed int32
}

// NewSorter creates a new Sorter
func NewSorter(
	ctx context.Context, tableID int64, startTs uint64,
	dbRouter *actor.Router[message.Task], dbActorID actor.ID,
	writerSystem *actor.System[message.Task], writerRouter *actor.Router[message.Task],
	readerSystem *actor.System[message.Task], readerRouter *actor.Router[message.Task],
	compact *CompactScheduler, cfg *config.DBConfig,
) (*Sorter, error) {
	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)
	metricIterDuration := sorterIterReadDurationHistogram.MustCurryWith(
		prometheus.Labels{
			"namespace": changefeedID.Namespace,
			"id":        changefeedID.ID,
		})
	metricInputKV := sorter.InputEventCount.
		WithLabelValues(changefeedID.Namespace, changefeedID.ID, "kv")
	metricInputResolved := sorter.InputEventCount.
		WithLabelValues(changefeedID.Namespace, changefeedID.ID, "resolved")
	metricOutputKV := sorter.OutputEventCount.
		WithLabelValues(changefeedID.Namespace, changefeedID.ID, "kv")
	metricOutputResolved := sorter.InputEventCount.
		WithLabelValues(changefeedID.Namespace, changefeedID.ID, "resolved")

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
		closedWg:  &sync.WaitGroup{},
	}

	w := &writer{
		common:        c,
		readerRouter:  readerRouter,
		readerActorID: actorID,

		metricTotalEventsKV:       metricInputKV,
		metricTotalEventsResolved: metricInputResolved,
	}
	wmb := actor.NewMailbox[message.Task](actorID, sorterInputCap)
	err := writerSystem.Spawn(wmb, w)
	if err != nil {
		return nil, errors.Trace(err)
	}
	c.closedWg.Add(1)

	r := &reader{
		common: c,

		state: pollState{
			outputBuf: newOutputBuffer(batchReceiveEventSize),

			maxCommitTs:   uint64(0),
			maxResolvedTs: uint64(0),

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
		outputCh:           make(chan *model.PolymorphicEvent, sorterOutputCap),

		metricIterReadDuration:    metricIterDuration.WithLabelValues("read"),
		metricIterNextDuration:    metricIterDuration.WithLabelValues("next"),
		metricTotalEventsKV:       metricOutputKV,
		metricTotalEventsResolved: metricOutputResolved,
	}
	rmb := actor.NewMailbox[message.Task](actorID, sorterInputCap)
	err = readerSystem.Spawn(rmb, r)
	if err != nil {
		return nil, errors.Trace(err)
	}
	c.closedWg.Add(1)

	return &Sorter{
		common:        c,
		writerRouter:  writerRouter,
		writerActorID: actorID,
		readerRouter:  readerRouter,
		ReaderActorID: actorID,
		outputCh:      r.outputCh,
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
	atomic.StoreInt32(&ls.closed, 1)
	// We should never lost message, make sure StopMessage is sent.
	ctx1 := context.TODO()
	// As the context can't be cancelled. SendB can only return an error
	// ActorStopped or ActorNotFound, and they mean actors have closed.
	_ = ls.writerRouter.SendB(
		ctx1, ls.writerActorID, actormsg.StopMessage[message.Task]())
	_ = ls.readerRouter.SendB(
		ctx1, ls.ReaderActorID, actormsg.StopMessage[message.Task]())
	ls.closedWg.Wait()

	_ = ls.cleanup(ctx1)
	return errors.Trace(err)
}

// AddEntry adds an RawKVEntry to the EntryGroup
func (ls *Sorter) AddEntry(ctx context.Context, event *model.PolymorphicEvent) {
	if atomic.LoadInt32(&ls.closed) != 0 {
		return
	}
	msg := actormsg.ValueMessage(message.Task{
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
	msg := actormsg.ValueMessage(message.Task{
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
	msg := actormsg.ValueMessage(message.Task{
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
	_ = ls.readerRouter.Send(ls.ReaderActorID, msg)
	return ls.outputCh
}

// cleanup cleans up sorter's data.
func (ls *Sorter) cleanup(ctx context.Context) error {
	task := message.Task{UID: ls.uid, TableID: ls.tableID}
	task.DeleteReq = &message.DeleteRequest{
		// We do not set task.Delete.Count, because we don't know
		// how many key-value pairs in the range.
		Range: [2][]byte{
			encoding.EncodeTsKey(ls.uid, ls.tableID, 0),
			encoding.EncodeTsKey(ls.uid, ls.tableID+1, 0),
		},
	}
	return ls.dbRouter.SendB(ctx, ls.dbActorID, actormsg.ValueMessage(task))
}
