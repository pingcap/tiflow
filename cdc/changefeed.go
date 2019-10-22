// Copyright 2019 PingCAP, Inc.
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

package cdc

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb-cdc/cdc/kv"
	"github.com/pingcap/tidb-cdc/cdc/util"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type formatType string

const (
	optFormat = "format"

	optFormatJSON formatType = "json"
)

type emitEntry struct {
	row *encodeRow

	resolved *ResolvedSpan
}

// ChangeFeedDetail describe the detail of a ChangeFeed
type ChangeFeedDetail struct {
	SinkURI      string            `json:"sink-uri"`
	Opts         map[string]string `json:"opts"`
	CheckpointTS uint64            `json:"checkpoint-ts"`
	CreateTime   time.Time         `json:"create-time"`
	// All events with CommitTS <= ResolvedTS can be synchronized
	// ResolvedTS is updated by owner only
	ResolvedTS uint64 `json:"resolved-ts"`
	// The ChangeFeed will exits until sync to timestamp TargetTS
	TargetTS uint64 `json:"target-ts"`
}

func (cfd *ChangeFeedDetail) String() string {
	data, err := json.Marshal(cfd)
	if err != nil {
		log.Error("fail to marshal ChangeFeedDetail to json", zap.Error(err))
	}
	return string(data)
}

// DecodeChangeFeedDetail decodes a new ChangeFeedDetail instance from json marshal byte slice
func DecodeChangeFeedDetail(data []byte) (ChangeFeedDetail, error) {
	detail := ChangeFeedDetail{}
	err := json.Unmarshal(data, &detail)
	return detail, errors.Trace(err)
}

// SaveChangeFeedDetail stores change feed detail into etcd
// TODO: this should be called from outer system, such as from a TiDB client
func (cfd *ChangeFeedDetail) SaveChangeFeedDetail(ctx context.Context, client *clientv3.Client, changeFeedID string) error {
	key := getEtcdKeyChangeFeed(changeFeedID)
	_, err := client.Put(ctx, key, cfd.String())
	return err
}

// SubChangeFeed is a SubChangeFeed task on capture
type SubChangeFeed struct {
	pdEndpoints []string
	pdCli       pd.Client
	detail      ChangeFeedDetail
	watchs      []util.Span

	schema  *Schema
	mounter *TxnMounter

	// sink is the Sink to write rows to.
	// Resolved timestamps are never written by Capture
	sink Sink
}

func NewSubChangeFeed(pdEndpoints []string, detail ChangeFeedDetail) (*SubChangeFeed, error) {
	pdCli, err := pd.NewClient(pdEndpoints, pd.SecurityOption{})
	if err != nil {
		return nil, errors.Annotatef(err, "create pd client failed, addr: %v", pdEndpoints)
	}

	// here we create another pb client,we should reuse them
	kvStore, err := createTiStore(strings.Join(pdEndpoints, ","))
	if err != nil {
		return nil, err
	}
	jobs, err := kv.LoadHistoryDDLJobs(kvStore)
	if err != nil {
		return nil, err
	}
	schema, err := NewSchema(jobs, false)
	if err != nil {
		return nil, errors.Trace(err)
	}

	sink, err := getSink(detail.SinkURI, schema, detail.Opts)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// TODO: get time zone from config
	mounter, err := NewTxnMounter(schema, time.UTC)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &SubChangeFeed{
		pdEndpoints: pdEndpoints,
		detail:      detail,
		pdCli:       pdCli,
		schema:      schema,
		sink:        sink,
		mounter:     mounter,
	}, nil
}

func (c *SubChangeFeed) Start(ctx context.Context, result chan<- error) {
	errCh := make(chan error, 1)

	ddlSpan := util.Span{
		Start: []byte{'m'},
		End:   []byte{'m' + 1},
	}
	ddlPuller := c.startOnSpan(ctx, ddlSpan, errCh)

	tblSpan := util.Span{
		Start: []byte{'t'},
		End:   []byte{'t' + 1},
	}
	dmlPuller := c.startOnSpan(ctx, tblSpan, errCh)

	// TODO: Set up a way to notify the pullers of new resolved ts
	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if err != context.Canceled {
				result <- err
			}
			return
		case e := <-errCh:
			result <- e
			return
		case <-time.After(10 * time.Millisecond):
			ts := c.GetResolvedTs(ddlPuller, dmlPuller)
			// NOTE: prevent too much noisy log now, refine it later
			if ts > 0 {
				log.Info("Min ResolvedTs", zap.Uint64("ts", ts))
			}
		}
	}
}

func (c *SubChangeFeed) GetResolvedTs(pullers ...*Puller) uint64 {
	minResolvedTs := pullers[0].GetResolvedTs()
	for _, p := range pullers[1:] {
		ts := p.GetResolvedTs()
		if ts < minResolvedTs {
			minResolvedTs = ts
		}
	}
	return minResolvedTs
}

func (c *SubChangeFeed) startOnSpan(ctx context.Context, span util.Span, errCh chan<- error) *Puller {
	// Set it up so that one failed goroutine cancels all others sharing the same ctx
	errg, ctx := errgroup.WithContext(ctx)

	checkpointTS := c.detail.CheckpointTS
	if checkpointTS == 0 {
		checkpointTS = oracle.EncodeTSO(c.detail.CreateTime.Unix() * 1000)
	}

	puller := NewPuller(c.pdCli, checkpointTS, []util.Span{span}, c.detail)

	errg.Go(func() error {
		return puller.Run(ctx)
	})

	errg.Go(func() error {
		return puller.CollectRawTxns(ctx, c.writeToSink)
	})

	go func() {
		err := errg.Wait()
		errCh <- err
	}()

	return puller
}

func (c *SubChangeFeed) writeToSink(context context.Context, rawTxn RawTxn) error {
	log.Info("RawTxn", zap.Reflect("RawTxn", rawTxn.entries))
	txn, err := c.mounter.Mount(rawTxn)
	if err != nil {
		return errors.Trace(err)
	}
	err = c.sink.Emit(context, *txn)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("Output Txn", zap.Reflect("Txn", txn))
	return nil
}

// kvsToRows gets changed kvs from a closure and converts them into sql rows.
// The returned closure is not threadsafe.
func kvsToRows(
	detail ChangeFeedDetail,
	inputFn func(context.Context) (BufferEntry, error),
) func(context.Context) (*emitEntry, error) {
	panic("todo")
}

// emitEntries connects to a sink, receives rows from a closure, and repeatedly
// emits them to the sink. It returns a closure that may be repeatedly called to
// advance the changefeed and which returns span-level resolved timestamp
// updates. The returned closure is not threadsafe.
func emitEntries(
	detail ChangeFeedDetail,
	watchedSpans []util.Span,
	encoder Encoder,
	sink Sink,
	inputFn func(context.Context) (*emitEntry, error),
) func(context.Context) ([]ResolvedSpan, error) {
	panic("todo")
}

// emitResolvedTimestamp emits a changefeed-level resolved timestamp
func emitResolvedTimestamp(
	ctx context.Context, encoder Encoder, sink Sink, resolved uint64,
) error {
	if err := sink.EmitResolvedTimestamp(ctx, encoder, resolved); err != nil {
		return err
	}

	log.Info("resolved", zap.Uint64("timestamp", resolved))

	return nil
}
