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

	"github.com/pingcap/ticdc/cdc/sink"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	checkTaskKeyInterval = time.Second * 1
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
	etcdCli     kv.CDCEtcdClient
	infos       map[string]model.ChangeFeedInfo
}

// NewChangeFeedWatcher creates a new changefeed watcher
func NewChangeFeedWatcher(captureID string, pdEndpoints []string, cli kv.CDCEtcdClient) *ChangeFeedWatcher {
	w := &ChangeFeedWatcher{
		captureID:   captureID,
		pdEndpoints: pdEndpoints,
		etcdCli:     cli,
		infos:       make(map[string]model.ChangeFeedInfo),
	}
	return w
}

func (w *ChangeFeedWatcher) processPutKv(kv *mvccpb.KeyValue) (bool, string, model.ChangeFeedInfo, error) {
	needRunWatcher := false
	changefeedID, err := util.ExtractKeySuffix(string(kv.Key))
	if err != nil {
		return needRunWatcher, "", model.ChangeFeedInfo{}, err
	}
	info := model.ChangeFeedInfo{}
	err = info.Unmarshal(kv.Value)
	if err != nil {
		return needRunWatcher, changefeedID, info, err
	}
	w.lock.Lock()
	_, ok := w.infos[changefeedID]
	if !ok {
		needRunWatcher = true
	}
	if info.AdminJobType == model.AdminStop {
		// only handle model.AdminStop, the model.AdminRemove case will be handled in `processDeleteKv`
		delete(w.infos, changefeedID)
	} else {
		w.infos[changefeedID] = info
	}
	w.lock.Unlock()
	// TODO: this info is not copied, should be readonly
	return needRunWatcher, changefeedID, info, nil
}

func (w *ChangeFeedWatcher) processDeleteKv(kv *mvccpb.KeyValue) error {
	changefeedID, err := util.ExtractKeySuffix(string(kv.Key))
	if err != nil {
		return errors.Trace(err)
	}
	w.lock.Lock()
	delete(w.infos, changefeedID)
	w.lock.Unlock()
	return nil
}

// Watch watches changefeed key base
func (w *ChangeFeedWatcher) Watch(ctx context.Context, cb processorCallback) error {
	errCh := make(chan error, 1)

	revision, infos, err := w.etcdCli.GetChangeFeeds(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	for changefeedID, kv := range infos {
		needRunWatcher, _, info, err := w.processPutKv(kv)
		if err != nil {
			return errors.Trace(err)
		}
		if needRunWatcher {
			_, err := runProcessorWatcher(ctx, changefeedID, w.captureID, w.pdEndpoints, w.etcdCli, info, errCh, cb)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	watchCh := w.etcdCli.Client.Watch(ctx, kv.GetEtcdKeyChangeFeedList(), clientv3.WithPrefix(), clientv3.WithRev(revision))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			return errors.Trace(err)
		case resp, ok := <-watchCh:
			if !ok {
				log.Info("watcher is closed")
				return nil
			}
			failpoint.Inject("WatchChangeFeedInfoCompactionErr", func() {
				failpoint.Return(errors.Trace(mvcc.ErrCompacted))
			})
			respErr := resp.Err()
			if respErr != nil {
				return errors.Trace(respErr)
			}
			for _, ev := range resp.Events {
				switch ev.Type {
				case mvccpb.PUT:
					needRunWatcher, changefeedID, info, err := w.processPutKv(ev.Kv)
					if err != nil {
						return errors.Trace(err)
					}
					if needRunWatcher {
						_, err := runProcessorWatcher(ctx, changefeedID, w.captureID, w.pdEndpoints, w.etcdCli, info, errCh, cb)
						if err != nil {
							return errors.Trace(err)
						}
					}
				case mvccpb.DELETE:
					err := w.processDeleteKv(ev.Kv)
					if err != nil {
						return errors.Trace(err)
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
	etcdCli      kv.CDCEtcdClient
	info         model.ChangeFeedInfo
	checkpointTs uint64
	wg           sync.WaitGroup
	closed       int32
}

// NewProcessorWatcher creates a new ProcessorWatcher instance
func NewProcessorWatcher(
	changefeedID string,
	captureID string,
	pdEndpoints []string,
	cli kv.CDCEtcdClient,
	info model.ChangeFeedInfo,
	checkpointTs uint64,
) *ProcessorWatcher {
	return &ProcessorWatcher{
		changefeedID: changefeedID,
		captureID:    captureID,
		pdEndpoints:  pdEndpoints,
		etcdCli:      cli,
		info:         info,
		checkpointTs: checkpointTs,
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

// Watch wait for the key `/changefeed/task/<fid>/cid>` appear and run the processor.
func (w *ProcessorWatcher) Watch(ctx context.Context, errCh chan<- error, cb processorCallback) {
	defer w.wg.Done()
	key := kv.GetEtcdKeyTaskStatus(w.changefeedID, w.captureID)

	getResp, err := w.etcdCli.Client.Get(ctx, key)
	if err != nil {
		errCh <- errors.Trace(err)
		return
	}
	if getResp.Count == 0 {
		rl := rate.NewLimiter(0.1, 5)
		revision := getResp.Header.Revision
		// wait for key to appear
		log.Info("waiting dispatching tasks",
			zap.String("key", key),
			zap.Int64("rev", revision))
		watchCh := w.etcdCli.Client.Watch(ctx, key, clientv3.WithRev(revision))
	waitKeyLoop:
		for {
			if !rl.Allow() {
				errCh <- errors.New("task key watcher exceeds rate limit")
				return
			}
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
					if respErr == mvcc.ErrCompacted {
						continue waitKeyLoop
					}
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
	err = runProcessor(cctx, w.pdEndpoints, w.info, w.changefeedID, w.captureID, w.checkpointTs, cb)
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
		case <-time.After(checkTaskKeyInterval):
			resp, err := w.etcdCli.Client.Get(ctx, key)
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

type processorCallback interface {
	// OnRunProcessor is called when the processor is started.
	OnRunProcessor(p *processor)
	// OnStopProcessor is called when the processor is stopped.
	OnStopProcessor(p *processor, err error)
}

// realRunProcessorWatcher creates a new ProcessorWatcher and executes the Watch method.
func realRunProcessorWatcher(
	ctx context.Context,
	changefeedID string,
	captureID string,
	pdEndpoints []string,
	etcdCli kv.CDCEtcdClient,
	info model.ChangeFeedInfo,
	errCh chan error,
	cb processorCallback,
) (*ProcessorWatcher, error) {
	status, err := etcdCli.GetChangeFeedStatus(ctx, changefeedID)
	if err != nil && errors.Cause(err) != model.ErrChangeFeedNotExists {
		return nil, errors.Trace(err)
	}
	checkpointTs := info.GetCheckpointTs(status)
	sw := NewProcessorWatcher(changefeedID, captureID, pdEndpoints, etcdCli, info, checkpointTs)
	ctx = util.PutChangefeedIDInCtx(ctx, changefeedID)
	sw.wg.Add(1)
	go sw.Watch(ctx, errCh, cb)
	return sw, nil
}

// realRunProcessor creates a new processor then starts it, and returns a channel to pass error.
func realRunProcessor(
	ctx context.Context,
	pdEndpoints []string,
	info model.ChangeFeedInfo,
	changefeedID string,
	captureID string,
	checkpointTs uint64,
	cb processorCallback,
) error {
	opts := make(map[string]string, len(info.Opts)+2)
	for k, v := range info.Opts {
		opts[k] = v
	}
	opts[sink.OptChangefeedID] = changefeedID
	opts[sink.OptCaptureID] = captureID
	filter, err := util.NewFilter(info.GetConfig())
	if err != nil {
		return errors.Trace(err)
	}
	sink, err := sink.NewSink(info.SinkURI, filter, opts)
	if err != nil {
		return errors.Trace(err)
	}
	ctx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		if err := sink.Run(ctx); errors.Cause(err) != context.Canceled {
			errCh <- err
		}
	}()
	processor, err := NewProcessor(ctx, pdEndpoints, info, sink, changefeedID, captureID, checkpointTs)
	if err != nil {
		cancel()
		return err
	}
	log.Info("start to run processor", zap.String("changefeed id", changefeedID))

	if cb != nil {
		cb.OnRunProcessor(processor)
	}

	processor.Run(ctx, errCh)

	go func() {
		err := <-errCh
		if cb != nil {
			cb.OnStopProcessor(processor, err)
		}
		cancel()
	}()

	return nil
}
