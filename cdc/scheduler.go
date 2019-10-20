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
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-cdc/cdc/kv"
)

var runSubChangeFeed = realRunSubChangeFeed

// ChangeFeedWatcher is a changefeed watcher
type ChangeFeedWatcher struct {
	captureID   string
	pdEndpoints []string
	etcdCli     *clientv3.Client
	details     map[string]ChangeFeedDetail
}

const (
	keyChangeFeedList int = iota + 1
	keyChangeFeed
	keySubChangeFeed
)

func getEtcdKey(keyType int, params ...interface{}) string {
	switch keyType {
	case keyChangeFeedList:
		return fmt.Sprintf("%s/changefeed/config", kv.EtcdKeyBase)
	case keyChangeFeed:
		return fmt.Sprintf("%s/changefeed/config/%s", append([]interface{}{kv.EtcdKeyBase}, params...)...)
	case keySubChangeFeed:
		return fmt.Sprintf("%s/changefeed/subchangfeed/%s/%s", append([]interface{}{kv.EtcdKeyBase}, params...)...)
	default:
		return "unknonw"
	}
}

func splitChangeFeedID(key string) string {
	subs := strings.Split(key, "/")
	return subs[len(subs)-1]
}

// NewChangeFeedWatcher creates a new changefeed watcher
func NewChangeFeedWatcher(captureID string, pdEndpoints []string, cli *clientv3.Client) *ChangeFeedWatcher {
	w := &ChangeFeedWatcher{
		captureID:   captureID,
		pdEndpoints: pdEndpoints,
		etcdCli:     cli,
		details:     make(map[string]ChangeFeedDetail),
	}
	return w
}

// Watch watches changefeed key base
func (w *ChangeFeedWatcher) Watch(ctx context.Context) error {
	key := getEtcdKey(keyChangeFeedList)
	watchCh := w.etcdCli.Watch(ctx, key, clientv3.WithPrefix())
	errCh := make(chan error, 1)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			return err
		case resp, ok := <-watchCh:
			if !ok {
				log.Info("watcher is closed")
				return nil
			}
			for _, ev := range resp.Events {
				switch ev.Type {
				case mvccpb.PUT:
					changefeedID := splitChangeFeedID(string(ev.Kv.Key))
					detail, err := RestoreChangeFeedDetail(ev.Kv.Value)
					if err != nil {
						return errors.Trace(err)
					}
					_, ok := w.details[changefeedID]
					if !ok {
						sw := NewSubChangeFeedWatcher(changefeedID, w.captureID, w.pdEndpoints, w.etcdCli, detail)
						go sw.Watch(ctx, errCh)
					}
					w.details[changefeedID] = detail
				case mvccpb.DELETE:
					changefeedID := splitChangeFeedID(string(ev.Kv.Key))
					delete(w.details, changefeedID)
				}
			}
		}
	}
}

// SubChangeFeedWatcher is a subchangefeed watcher
type SubChangeFeedWatcher struct {
	pdEndpoints  []string
	changefeedID string
	captureID    string
	etcdCli      *clientv3.Client
	detail       ChangeFeedDetail
}

// NewSubChangeFeedWatcher creates a new SubChangeFeedWatcher instance
func NewSubChangeFeedWatcher(
	changefeedID string,
	captureID string,
	pdEndpoints []string,
	cli *clientv3.Client,
	detail ChangeFeedDetail,
) *SubChangeFeedWatcher {
	return &SubChangeFeedWatcher{
		changefeedID: changefeedID,
		captureID:    captureID,
		pdEndpoints:  pdEndpoints,
		etcdCli:      cli,
		detail:       detail,
	}
}

func (w *SubChangeFeedWatcher) Watch(ctx context.Context, errCh chan<- error) {
	key := getEtcdKey(keySubChangeFeed, w.changefeedID, w.captureID)

	val, err := w.etcdCli.Get(ctx, key)
	if err != nil {
		errCh <- errors.Trace(err)
		return
	}
	if val.Count == 0 {
		// wait for key to appear
		watchCh := w.etcdCli.Watch(ctx, key)
	waitKeyLoop:
		for {
			select {
			case <-ctx.Done():
				return
			case resp, ok := <-watchCh:
				if !ok {
					log.Info("watcher is closed")
					return
				}
				for _, ev := range resp.Events {
					switch ev.Type {
					case mvccpb.PUT:
						break waitKeyLoop
					}
				}
			}
		}
	}

	feedErrCh, err := runSubChangeFeed(ctx, w.pdEndpoints, w.detail)
	if err != nil {
		errCh <- err
		return
	}

	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if err != context.Canceled {
				errCh <- err
			}
			return
		case err := <-feedErrCh:
			errCh <- err
			return
		case <-time.After(time.Second):
			resp, err := w.etcdCli.Get(ctx, key)
			if err != nil {
				errCh <- errors.Trace(err)
			}
			// subchangefeed has been removed from this capture
			if resp.Count == 0 {
				return
			}
		}
	}
}

// realRunSubChangeFeed creates a new subchangefeed then starts it, and returns a channel to pass error
func realRunSubChangeFeed(ctx context.Context, pdEndpoints []string, detail ChangeFeedDetail) (chan error, error) {
	feed, err := NewSubChangeFeed(pdEndpoints, detail)
	if err != nil {
		return nil, err
	}
	errCh := make(chan error, 1)
	go feed.Start(ctx, errCh)
	return errCh, nil
}
