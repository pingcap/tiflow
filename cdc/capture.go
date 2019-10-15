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

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-cdc/cdc/kv"
	"github.com/pingcap/tidb-cdc/cdc/util"
	"github.com/pingcap/tidb-cdc/pkg/flags"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store"
	"github.com/pingcap/tidb/store/tikv"
)

const (
	// CaptureOwnerKey is the capture owner path that is saved to etcd
	CaptureOwnerKey = kv.EtcdKeyBase + "/capture/owner"
)

type ResolvedSpan struct {
	Span      util.Span
	Timestamp uint64
}

// Capture represents a Capture server, it monitors the changefeed information in etcd and schedules SubChangeFeed on it.
type Capture struct {
	id string
}

// NewCapture returns a new Capture instance
func NewCapture() (c *Capture, err error) {
	c = &Capture{
		id: uuid.New().String(),
	}

	return
}

// Start starts the Capture mainloop
func (c *Capture) Start(ctx context.Context) (err error) {
	// TODO: better channgefeed model with etcd storage
	err = c.register()
	if err != nil {
		return err
	}

	// create a test changefeed
	detail := ChangeFeedDetail{
		SinkURI:      "root@tcp(127.0.0.1:3306)/test",
		Opts:         make(map[string]string),
		CheckpointTS: 0,
		CreateTime:   time.Now(),
	}
	feed, err := NewSubChangeFeed([]string{"localhost:2379"}, detail)
	if err != nil {
		return errors.Trace(err)
	}
	return feed.Start(ctx)
}

// register registers the capture information in etcd
func (c *Capture) register() error {
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
