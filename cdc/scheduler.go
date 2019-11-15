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
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-cdc/cdc/kv"
	"github.com/pingcap/tidb-cdc/cdc/model"
	"github.com/pingcap/tidb-cdc/pkg/util"
)

var (
	runProcessorWatcher = realRunProcessorWatcher
	runProcessor        = realRunProcessor
)

// ChangeFeedWatcher is a changefeed watcher
type ChangeFeedWatcher struct {
	lock        sync.RWMutex
	captureID   string
	pdEndpoints []string
	etcdCli     *clientv3.Client
	details     map[string]model.ChangeFeedDetail
}

// NewChangeFeedWatcher creates a new changefeed watcher
func NewChangeFeedWatcher(captureID string, pdEndpoints []string, cli *clientv3.Client) *ChangeFeedWatcher {
	w := &ChangeFeedWatcher{
		captureID:   captureID,
		pdEndpoints: pdEndpoints,
		etcdCli:     cli,
		details:     make(map[string]model.ChangeFeedDetail),
	}
	return w
}

func (w *ChangeFeedWatcher) processPutKv(kv *mvccpb.KeyValue) (bool, string, model.ChangeFeedDetail, error) {
	needRunWatcher := false
	changefeedID, err := util.ExtractKeySuffix(string(kv.Key))
	if err != nil {
		return needRunWatcher, "", model.ChangeFeedDetail{}, err
	}
	detail := model.ChangeFeedDetail{}
	err = detail.Unmarshal(kv.Value)
	if err != nil {
		return needRunWatcher, changefeedID, detail, err
	}
	w.lock.Lock()
	_, ok := w.details[changefeedID]
	if !ok {
		needRunWatcher = true
	}
	w.details[changefeedID] = detail
	w.lock.Unlock()
	// TODO: this detail is not copied, should be readonly
	return needRunWatcher, changefeedID, detail, nil
}

func (w *ChangeFeedWatcher) processDeleteKv(kv *mvccpb.KeyValue) error {
	changefeedID, err := util.ExtractKeySuffix(string(kv.Key))
	if err != nil {
		return err
	}
	w.lock.Lock()
	delete(w.details, changefeedID)
	w.lock.Unlock()
	return nil
}

// Watch watches changefeed key base
func (w *ChangeFeedWatcher) Watch(ctx context.Context) error {
	errCh := make(chan error, 1)

	revision, details, err := kv.GetChangeFeeds(ctx, w.etcdCli)
	if err != nil {
		return err
	}
	for changefeedID, kv := range details {
		needRunWatcher, _, detail, err := w.processPutKv(kv)
		if err != nil {
			return err
		}
		if needRunWatcher {
			runProcessorWatcher(ctx, changefeedID, w.captureID, w.pdEndpoints, w.etcdCli, detail, errCh)
		}
	}

	watchCh := w.etcdCli.Watch(ctx, kv.GetEtcdKeyChangeFeedList(), clientv3.WithPrefix(), clientv3.WithRev(revision))
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
			respErr := resp.Err()
			if respErr != nil {
				return errors.Trace(respErr)
			}
			for _, ev := range resp.Events {
				switch ev.Type {
				case mvccpb.PUT:
					needRunWatcher, changefeedID, detail, err := w.processPutKv(ev.Kv)
					if err != nil {
						return err
					}
					if needRunWatcher {
						runProcessorWatcher(ctx, changefeedID, w.captureID, w.pdEndpoints, w.etcdCli, detail, errCh)
					}
				case mvccpb.DELETE:
					err := w.processDeleteKv(ev.Kv)
					if err != nil {
						return err
					}
				}
			}
		}
	}
}

// ProcessorWatcher is a processor watcher
type ProcessorWatcher struct {
	pdEndpoints  []string
	changefeedID string
	captureID    string
	etcdCli      *clientv3.Client
	detail       model.ChangeFeedDetail
	wg           sync.WaitGroup
	closed       int32
}

// NewProcessorWatcher creates a new ProcessorWatcher instance
func NewProcessorWatcher(
	changefeedID string,
	captureID string,
	pdEndpoints []string,
	cli *clientv3.Client,
	detail model.ChangeFeedDetail,
) *ProcessorWatcher {
	return &ProcessorWatcher{
		changefeedID: changefeedID,
		captureID:    captureID,
		pdEndpoints:  pdEndpoints,
		etcdCli:      cli,
		detail:       detail,
	}
}

func (w *ProcessorWatcher) isClosed() bool {
	return atomic.LoadInt32(&w.closed) == 1
}

func (w *ProcessorWatcher) close() {
	atomic.StoreInt32(&w.closed, 1)
	w.wg.Wait()
}

func (w *ProcessorWatcher) reopen() error {
	if !w.isClosed() {
		return errors.New("ProcessorWatcher is not closed")
	}
	atomic.StoreInt32(&w.closed, 0)
	return nil
}

// Watch wait for the key `/changefeed/subchangefeed/<fid>/cid>` appear and run the processor.
func (w *ProcessorWatcher) Watch(ctx context.Context, errCh chan<- error) {
	defer w.wg.Done()
	key := kv.GetEtcdKeySubChangeFeed(w.changefeedID, w.captureID)

	getResp, err := w.etcdCli.Get(ctx, key)
	if err != nil {
		errCh <- errors.Trace(err)
		return
	}
	revision := getResp.Header.Revision
	if getResp.Count == 0 {
		// wait for key to appear
		watchCh := w.etcdCli.Watch(ctx, key, clientv3.WithRev(revision))
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
				respErr := resp.Err()
				if respErr != nil {
					errCh <- errors.Trace(respErr)
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

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	feedErrCh, err := runProcessor(cctx, w.pdEndpoints, w.detail, w.changefeedID, w.captureID)
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
				return
			}
			// processor has been removed from this capture
			if resp.Count == 0 {
				return
			}
		}
	}
}

// realRunProcessorWatcher creates a new ProcessorWatcher and executes the Watch method.
func realRunProcessorWatcher(
	ctx context.Context,
	changefeedID string,
	captureID string,
	pdEndpoints []string,
	etcdCli *clientv3.Client,
	detail model.ChangeFeedDetail,
	errCh chan error,
) *ProcessorWatcher {
	sw := NewProcessorWatcher(changefeedID, captureID, pdEndpoints, etcdCli, detail)
	sw.wg.Add(1)
	go sw.Watch(ctx, errCh)
	return sw
}

// realRunProcessor creates a new processor then starts it, and returns a channel to pass error.
func realRunProcessor(
	ctx context.Context,
	pdEndpoints []string,
	detail model.ChangeFeedDetail,
	changefeedID string,
	captureID string,
) (chan error, error) {
	processor, err := NewProcessor(pdEndpoints, detail, changefeedID, captureID)
	if err != nil {
		return nil, err
	}
	errCh := make(chan error, 1)
	processor.Run(ctx, errCh)
	return errCh, nil
}
