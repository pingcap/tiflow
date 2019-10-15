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
	"fmt"
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

// SaveChangeFeedDetail stores change feed detail into etcd
// TODO: this should be called from outer system, such as from a TiDB client
func (cfd *ChangeFeedDetail) SaveChangeFeedDetail(ctx context.Context, client *clientv3.Client, changeFeedID string) error {
	key := fmt.Sprintf("/tidb/cdc/changefeed/%s/config", changeFeedID)
	_, err := client.Put(ctx, key, cfd.String())
	return err
}

// SubChangeFeed is a SubChangeFeed task on capture
type SubChangeFeed struct {
	pdCli    pd.Client
	detail   ChangeFeedDetail
	frontier *Frontier
	watchs   []util.Span

	// errCh contains the return values of the puller
	errCh chan error

	schema *Schema
	// sink is the Sink to write rows to.
	// Resolved timestamps are never written by Capture
	sink Sink
}

func NewSubChangeFeed(pdAddr []string, detail ChangeFeedDetail) (*SubChangeFeed, error) {
	pdCli, err := pd.NewClient(pdAddr, pd.SecurityOption{})
	if err != nil {
		return nil, errors.Annotatef(err, "create pd client failed, addr: %v", pdAddr)
	}
	return &SubChangeFeed{
		detail: detail,
		pdCli:  pdCli,
	}, nil
}

func (c *SubChangeFeed) Start(ctx context.Context) error {
	checkpointTS := c.detail.CheckpointTS
	if checkpointTS == 0 {
		checkpointTS = oracle.EncodeTSO(c.detail.CreateTime.Unix() * 1000)
	}

	// TODO: just one SubChangeFeed watch all kv for test now
	c.watchs = []util.Span{{nil, nil}}

	var err error
	c.frontier, err = NewFrontier(c.watchs, c.detail)
	if err != nil {
		return errors.Annotate(err, "NewFrontier failed")
	}

	// encoder, err := getEncoder(c.detail.Opts)
	// if err != nil {
	// 	return err
	// }

	// TODO get etdc url from config
	// here we create another pb client,we should reuse them
	kvStore, err := createTiStore("http://localhost:2379")
	if err != nil {
		return errors.Trace(err)
	}
	jobs, err := kv.LoadHistoryDDLJobs(kvStore)
	if err != nil {
		return errors.Trace(err)
	}
	schema, err := NewSchema(jobs, false)
	if err != nil {
		return errors.Trace(err)
	}

	sink, err := getSink(c.detail.SinkURI, schema, c.detail.Opts)
	if err != nil {
		return errors.Trace(err)
	}
	c.sink = sink

	errg, ctx := errgroup.WithContext(context.Background())

	errg.Go(func() error {
		return c.run(ctx)
	})

	// errg.Go(func() error {
	// 	return frontier.Start(ctx)
	// })

	return errg.Wait()
}

func (c *SubChangeFeed) run(ctx context.Context) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	buf := MakeBuffer()

	// TODO get time zone from config
	mounter, err := NewTxnMounter(c.schema, time.UTC)
	if err != nil {
		return errors.Trace(err)
	}

	puller := NewPuller(c.pdCli, c.detail.CheckpointTS, c.watchs, c.detail, buf)
	c.errCh = make(chan error, 2)
	go func() {
		err := puller.Run(cctx)
		c.errCh <- err
	}()

	spanFrontier := makeSpanFrontier(c.watchs...)

	writeToSink := func(context context.Context, rawTxn RawTxn) error {
		log.Info("RawTxn", zap.Reflect("RawTxn", rawTxn.entries))
		txn, err := mounter.Mount(rawTxn)
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

	err = collectRawTxns(cctx, buf.Get, writeToSink, spanFrontier)
	if err != nil {
		return errors.Trace(err)
	}

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
