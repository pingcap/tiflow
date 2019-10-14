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

type ChangeFeed struct {
	pdCli    pd.Client
	detail   ChangeFeedDetail
	frontier *Frontier
}

func NewChangeFeed(pdAddr []string, detail ChangeFeedDetail) (*ChangeFeed, error) {
	pdCli, err := pd.NewClient(pdAddr, pd.SecurityOption{})
	if err != nil {
		return nil, errors.Annotatef(err, "create pd client failed, addr: %v", pdAddr)
	}
	return &ChangeFeed{
		detail: detail,
		pdCli:  pdCli,
	}, nil
}

func (c *ChangeFeed) Start(ctx context.Context) error {
	checkpointTS := c.detail.CheckpointTS
	if checkpointTS == 0 {
		checkpointTS = oracle.EncodeTSO(c.detail.CreateTime.Unix() * 1000)
	}

	var err error
	c.frontier, err = NewFrontier([]util.Span{{nil, nil}}, c.detail)
	if err != nil {
		return errors.Annotate(err, "NewFrontier failed")
	}

	// TODO: just one capture watch all kv for test now
	capture, err := NewCapture(c.pdCli, []util.Span{{nil, nil}}, checkpointTS, c.detail)
	if err != nil {
		return errors.Annotate(err, "NewCapture failed")
	}

	errg, ctx := errgroup.WithContext(context.Background())

	errg.Go(func() error {
		return capture.Start(ctx)
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
