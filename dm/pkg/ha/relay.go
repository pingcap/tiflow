// Copyright 2021 PingCAP, Inc.
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

package ha

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/common"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

// RelaySource represents the bound relationship between the DM-worker instance and its upstream relay source.
type RelaySource struct {
	Source string
	// only used to report to the caller of the watcher, do not marsh it.
	// if it's true, it means the bound has been deleted in etcd.
	IsDeleted bool
	// record the etcd ModRevision of this bound
	Revision int64
}

// PutRelayConfig puts the relay config for given workers.
// k/v: worker-name -> source-id.
// TODO: let caller wait until worker has enabled relay.
func PutRelayConfig(cli *clientv3.Client, bounds ...SourceBound) (int64, error) {
	ops := make([]clientv3.Op, 0, len(bounds))
	for _, bound := range bounds {
		ops = append(ops, putRelayConfigOp(bound))
	}
	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(ops...))
	return rev, err
}

// DeleteRelayConfig deletes the relay config for given workers.
func DeleteRelayConfig(cli *clientv3.Client, source string, workers ...string) (int64, error) {
	ops := make([]clientv3.Op, 0, len(workers))
	for _, worker := range workers {
		ops = append(ops, deleteRelayConfigOp(NewSourceBound(source, worker)))
	}
	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(ops...))
	return rev, err
}

// GetAllRelayConfig gets all source and its relay worker.
// k/v: source ID -> set(workers).
func GetAllRelayConfig(cli *clientv3.Client) (map[string]map[string]struct{}, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Get(ctx, common.UpstreamRelayWorkerKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return nil, 0, terror.ErrHAFailTxnOperation.Delegate(err, "fail to get all relay config")
	}

	ret := map[string]map[string]struct{}{}
	for _, kv := range resp.Kvs {
		keys, err2 := common.UpstreamRelayWorkerKeyAdapter.Decode(string(kv.Key))
		if err2 != nil {
			return nil, 0, err2
		}
		if len(keys) != 2 {
			// should not happened
			return nil, 0, terror.ErrDecodeEtcdKeyFail.Generate("illegal key of UpstreamRelayWorkerKeyAdapter")
		}
		worker, source := keys[0], keys[1]
		var (
			ok      bool
			workers map[string]struct{}
		)
		if workers, ok = ret[source]; !ok {
			workers = map[string]struct{}{}
			ret[source] = workers
		}
		workers[worker] = struct{}{}
	}
	return ret, resp.Header.Revision, nil
}

// GetAllRelayConfigBeforeV620 gets all upstream relay configs before v6.2.0.
// This func only use for config export command.
func GetAllRelayConfigBeforeV620(cli *clientv3.Client) (map[string]map[string]struct{}, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Get(ctx, common.UpstreamRelayWorkerKeyAdapterV1.Path(), clientv3.WithPrefix())
	if err != nil {
		return nil, 0, err
	}

	ret := map[string]map[string]struct{}{}
	for _, kv := range resp.Kvs {
		keys, err2 := common.UpstreamRelayWorkerKeyAdapterV1.Decode(string(kv.Key))
		if err2 != nil {
			return nil, 0, err2
		}
		if len(keys) != 1 {
			// should not happened
			return nil, 0, terror.ErrDecodeEtcdKeyFail.Generate("illegal key of UpstreamRelayWorkerKeyAdapterV1")
		}
		worker, source := keys[0], string(kv.Value)
		var (
			ok      bool
			workers map[string]struct{}
		)
		if workers, ok = ret[source]; !ok {
			workers = map[string]struct{}{}
			ret[source] = workers
		}
		workers[worker] = struct{}{}
	}

	return ret, resp.Header.Revision, nil
}

// GetRelayConfig returns the source config which the given worker need to pull relay log from etcd, with revision.
func GetRelayConfig(cli *clientv3.Client, worker string) (map[string]*config.SourceConfig, int64, error) {
	var (
		sources    []string
		newSources []string
		rev        int64
		retryNum   = defaultGetRelayConfigRetry
	)
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	getSourceIDsFromResp := func(resp *clientv3.GetResponse) ([]string, int64, error) {
		if resp.Count == 0 {
			return nil, resp.Header.Revision, nil
		}
		sourceIDs := make([]string, 0, resp.Count)
		// get all sourceID
		for _, kv := range resp.Kvs {
			keys, err2 := common.UpstreamRelayWorkerKeyAdapter.Decode(string(kv.Key))
			if err2 != nil {
				return nil, resp.Header.Revision, err2
			}
			if len(keys) != 2 {
				// should not happened
				return nil, resp.Header.Revision, terror.ErrDecodeEtcdKeyFail.Generate("illegal key of UpstreamRelayWorkerKeyAdapter")
			}
			sourceIDs = append(sourceIDs, keys[1])
		}
		return sourceIDs, resp.Header.Revision, nil
	}

	resp, err := cli.Get(ctx, common.UpstreamRelayWorkerKeyAdapter.Encode(worker), clientv3.WithPrefix())
	if err != nil {
		return nil, 0, terror.ErrHAFailTxnOperation.Delegate(err, "fail to get relay config")
	}
	sources, rev, err = getSourceIDsFromResp(resp)
	if err != nil || len(sources) == 0 {
		return nil, rev, err
	}

	appendGetUpstreamCfgOps := func(sources []string, ops []clientv3.Op) []clientv3.Op {
		for _, source := range sources {
			ops = append(ops, clientv3.OpGet(common.UpstreamConfigKeyAdapter.Encode(source)))
		}
		return ops
	}

	getSourceCfgFromResp := func(txnResp *clientv3.TxnResponse) (map[string]*config.SourceConfig, error) {
		if txnResp == nil || len(txnResp.Responses) < 2 {
			return nil, nil
		}
		scm := make(map[string]*config.SourceConfig, 0)
		for i := 1; i < len(txnResp.Responses); i++ {
			cfgResp := txnResp.Responses[i].GetResponseRange()
			scs, err1 := sourceCfgFromResp("", (*clientv3.GetResponse)(cfgResp))
			if err1 != nil {
				return nil, err1
			}
			for src, conf := range scs {
				scm[src] = conf
			}
		}
		return scm, nil
	}

	for retryCnt := 1; retryCnt <= retryNum; retryCnt++ {
		ops := make([]clientv3.Op, 1, len(sources)+1)
		ops[0] = clientv3.OpGet(common.UpstreamRelayWorkerKeyAdapter.Encode(worker), clientv3.WithPrefix())
		ops = appendGetUpstreamCfgOps(sources, ops)
		txnResp, _, err2 := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(ops...))
		if err2 != nil {
			return nil, 0, err2
		}

		var rev2 int64
		sourceResp := txnResp.Responses[0].GetResponseRange()
		newSources, rev2, err = getSourceIDsFromResp((*clientv3.GetResponse)(sourceResp))
		if err != nil {
			return nil, 0, err
		}
		// 1. newSources and sources are exactly the same, find source configs and return.
		// 2. newSources and sources are not the same, retry, when last retry is still not the same, for loop end and return error.
		newSourcesLen := len(newSources)
		sourcesLen := len(sources)
		if newSourcesLen == 0 && sourcesLen == 0 {
			return nil, rev2, nil
		}
		hasDiff := false
		if newSourcesLen == sourcesLen {
			for _, newSource := range newSources {
				for _, source := range sources {
					if newSource != source {
						hasDiff = true
						break
					}
				}
				if hasDiff {
					break
				}
			}
		}
		// not exactly the same, will retry
		if newSourcesLen != sourcesLen || hasDiff {
			log.L().Warn("relay config has been changed, will take a retry",
				zap.Strings("old relay sources", sources),
				zap.Strings("new relay sources", newSources),
				zap.Int("retryTime", retryCnt))
			// if we are about to fail, don't update relay source to save the last source to error
			if retryCnt != retryNum {
				sources = newSources
			}
			select {
			case <-cli.Ctx().Done():
				retryNum = 0 // stop retry
			case <-time.After(retryInterval):
				// retryInterval shouldn't be too long because the longer we wait, bound is more
				// possible to be different from newBound
			}
			if retryCnt != retryNum {
				continue
			}
		}

		// after retry and already the same, find source configs and return
		scm, err3 := getSourceCfgFromResp(txnResp)
		if err3 != nil {
			return nil, 0, err3
		}
		configs := make(map[string]*config.SourceConfig, 0)
		for _, sourceID := range sources {
			cfg, ok := scm[sourceID]
			// ok == false means we have got relay source but there is no source config, this shouldn't happen
			if !ok {
				// this should not happen.
				return nil, 0, terror.ErrConfigMissingForBound.Generate(sourceID)
			}
			configs[sourceID] = cfg
		}

		return configs, rev2, nil
	}
	return nil, 0, terror.ErrWorkerRelayConfigChanging.Generate(worker, sources, newSources)
}

// putRelayConfigOp returns PUT etcd operations for the relay relationship of the specified DM-worker.
// k/v: (worker-name, source-id) -> source-id.
func putRelayConfigOp(bound SourceBound) clientv3.Op {
	return clientv3.OpPut(common.UpstreamRelayWorkerKeyAdapter.Encode(bound.Worker, bound.Source), bound.Source)
}

// deleteRelayConfigOp returns a DELETE etcd operation for the relay relationship of the specified DM-worker.
func deleteRelayConfigOp(bound SourceBound) clientv3.Op {
	return clientv3.OpDelete(common.UpstreamRelayWorkerKeyAdapter.Encode(bound.Worker, bound.Source))
}

// WatchRelayConfig watches PUT & DELETE operations for the relay relationship of the specified DM-worker.
// For the DELETE operations, it returns an nil source config.
func WatchRelayConfig(ctx context.Context, cli *clientv3.Client,
	worker string, revision int64, outCh chan<- RelaySource, errCh chan<- error,
) {
	wCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch := cli.Watch(wCtx, common.UpstreamRelayWorkerKeyAdapter.Encode(worker), clientv3.WithRev(revision), clientv3.WithPrefix())

	for {
		select {
		case <-ctx.Done():
			return
		case resp, ok := <-ch:
			if !ok {
				return
			}
			if resp.Canceled {
				// TODO(csuzhangxc): do retry here.
				if resp.Err() != nil {
					select {
					case errCh <- terror.ErrHAFailWatchEtcd.Delegate(resp.Err(), fmt.Sprintf("watch relay config canceled, worker %s", worker)):
					case <-ctx.Done():
					}
				}
				return
			}

			for _, ev := range resp.Events {
				var bound RelaySource
				switch ev.Type {
				case mvccpb.PUT:
					bound.Source = string(ev.Kv.Value)
					bound.IsDeleted = false
				case mvccpb.DELETE:
					bound.IsDeleted = true
				default:
					// this should not happen.
					log.L().Error("unsupported etcd event type", zap.Reflect("kv", ev.Kv), zap.Reflect("type", ev.Type))
					continue
				}
				bound.Revision = ev.Kv.ModRevision

				select {
				case outCh <- bound:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}
