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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
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

type ChangeFeedDetail struct {
	SinkURI      string
	Opts         map[string]string
	CheckpointTS uint64
	CreateTime   time.Time
}

type ChangeFeed struct {
	detail   ChangeFeedDetail
	frontier *Frontier
}

func NewChangeFeed(detail ChangeFeedDetail) *ChangeFeed {
	return &ChangeFeed{
		detail: detail,
	}
}

func (c *ChangeFeed) Start(ctx context.Context) error {
	checkpointTS := c.detail.CheckpointTS
	if checkpointTS == 0 {
		checkpointTS = oracle.EncodeTSO(c.detail.CreateTime.Unix() * 1000)
	}

	var err error
	c.frontier, err = NewFrontier([]Span{{nil, nil}}, c.detail)
	if err != nil {
		return errors.Annotate(err, "NewFrontier failed")
	}

	// TODO: just one capture watch all kv for test now
	capure, err := NewCapture([]Span{{nil, nil}}, checkpointTS, c.detail)
	if err != nil {
		return errors.Annotate(err, "NewCapture failed")
	}

	errg, ctx := errgroup.WithContext(context.Background())

	errg.Go(func() error {
		return capure.Start(ctx)
	})

	// errg.Go(func() error {
	// 	return frontier.Start(ctx)
	// })

	return errg.Wait()
}

// kvsToRows gets changed kvs from a closure and converts them into sql rows.
// The returned closure is not threadsafe.
func kvsToRows(
	detail ChangeFeedDetail,
	inputFn func(context.Context) (bufferEntry, error),
) func(context.Context) (*emitEntry, error) {
	panic("todo")
}

// emitEntries connects to a sink, receives rows from a closure, and repeatedly
// emits them to the sink. It returns a closure that may be repeatedly called to
// advance the changefeed and which returns span-level resolved timestamp
// updates. The returned closure is not threadsafe.
func emitEntries(
	detail ChangeFeedDetail,
	watchedSpans []Span,
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
