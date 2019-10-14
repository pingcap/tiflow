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
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb-cdc/cdc/kv"
	"github.com/pingcap/tidb-cdc/cdc/util"
	"github.com/pingcap/tidb-cdc/pkg/flags"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store"
	"github.com/pingcap/tidb/store/tikv"
	"go.uber.org/zap"
)

const (
	// CaptureOwnerKey is the capture owner path that is saved to etcd
	CaptureOwnerKey = kv.EtcdKeyBase + "/capture/owner"
)

// Capture watch some span of KV and emit the entries to sink according to the ChangeFeedDetail
type Capture struct {
	id           string
	pdCli        pd.Client
	watchs       []util.Span
	checkpointTS uint64
	encoder      Encoder
	detail       ChangeFeedDetail

	// errCh contains the return values of the puller
	errCh  chan error
	cancel context.CancelFunc

	schema *Schema

	// sink is the Sink to write rows to.
	// Resolved timestamps are never written by Capture
	sink Sink
}

type ResolvedSpan struct {
	Span      util.Span
	Timestamp uint64
}

func NewCapture(
	pdCli pd.Client,
	watchs []util.Span,
	checkpointTS uint64,
	detail ChangeFeedDetail,
) (c *Capture, err error) {
	encoder, err := getEncoder(detail.Opts)
	if err != nil {
		return nil, err
	}

	// TODO get etdc url from config
	// here we create another pb client,we should reuse them
	kvStore, err := createTiStore("http://localhost:2379")
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs, err := kv.LoadHistoryDDLJobs(kvStore)
	if err != nil {
		return nil, errors.Trace(err)
	}
	schema, err := NewSchema(jobs, false)
	if err != nil {
		return nil, errors.Trace(err)
	}

	sink, err := getSink(detail.SinkURI, schema, detail.Opts)
	if err != nil {
		return nil, errors.Trace(err)
	}

	c = &Capture{
		pdCli:        pdCli,
		watchs:       watchs,
		checkpointTS: checkpointTS,
		encoder:      encoder,
		sink:         sink,
		detail:       detail,
		schema:       schema,
	}

	return
}

func (c *Capture) Start(ctx context.Context) (err error) {
	ctx, c.cancel = context.WithCancel(ctx)
	defer c.cancel()

	buf := MakeBuffer()

	// TODO get time zone from config
	mounter, err := NewTxnMounter(c.schema, time.UTC)
	if err != nil {
		return errors.Trace(err)
	}

	puller := NewPuller(c.pdCli, c.checkpointTS, c.watchs, c.detail, buf)
	c.errCh = make(chan error, 2)
	go func() {
		err := puller.Run(ctx)
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

	err = collectRawTxns(ctx, buf.Get, writeToSink, spanFrontier)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// register registers the capture information in etcd
func (c *Capture) register() error {
	return nil
}

// Frontier handle all ResolvedSpan and emit resolved timestamp
type Frontier struct {
	// once all the span receive a resolved ts, it's safe to emit a changefeed level resolved ts
	spans   []util.Span
	detail  ChangeFeedDetail
	encoder Encoder
}

func NewFrontier(spans []util.Span, detail ChangeFeedDetail) (f *Frontier, err error) {
	encoder, err := getEncoder(detail.Opts)
	if err != nil {
		return nil, err
	}

	f = &Frontier{
		spans:   spans,
		detail:  detail,
		encoder: encoder,
	}

	return
}

func (f *Frontier) NotifyResolvedSpan(resolve ResolvedSpan) error {
	// TODO emit resolved timestamp once it's safe

	return nil
}

func createTiStore(urls string) (tidbkv.Storage, error) {
	urlv, err := flags.NewURLsValue(urls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err := store.Register("tikv", tikv.Driver{}); err != nil {
		return nil, errors.Trace(err)
	}
	tiPath := fmt.Sprintf("tikv://%s?disableGC=true", urlv.HostString())
	tiStore, err := store.New(tiPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return tiStore, nil
}
