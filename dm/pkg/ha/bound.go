// Copyright 2020 PingCAP, Inc.
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
	"encoding/json"
	"fmt"
	"time"

	"github.com/pingcap/failpoint"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/common"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

const (
	// we need two steps to get a name/id and query config using that id.
	// since above steps can't be put into one etcd transaction, we combine and re-run the first step into the second
	// step, and check the name/id is still valid. if not valid, retry the second step using new name/id.
	defaultGetSourceBoundConfigRetry = 3
	defaultGetRelayConfigRetry       = 3
	retryInterval                    = 50 * time.Millisecond // retry interval when we get two different bounds
)

// SourceBound represents the bound relationship between the DM-worker instance and the upstream MySQL source.
type SourceBound struct {
	Source string `json:"source"` // the source ID of the upstream.
	Worker string `json:"worker"` // the name of the bounded DM-worker for the source.

	// only used to report to the caller of the watcher, do not marsh it.
	// if it's true, it means the bound has been deleted in etcd.
	IsDeleted bool `json:"-"`
	// record the etcd Revision of this bound
	Revision int64 `json:"-"`
}

// NewSourceBound creates a new SourceBound instance.
func NewSourceBound(source, worker string) SourceBound {
	return SourceBound{
		Source: source,
		Worker: worker,
	}
}

// String implements Stringer interface.
func (b SourceBound) String() string {
	s, _ := b.toJSON()
	return s
}

// toJSON returns the string of JSON represent.
func (b SourceBound) toJSON() (string, error) {
	data, err := json.Marshal(b)
	if err != nil {
		return "", terror.ErrHAInvalidItem.Delegate(err, fmt.Sprintf("fail to marshal SourceBound %+v", b))
	}
	return string(data), nil
}

// IsEmpty returns true when this bound has no value.
func (b SourceBound) IsEmpty() bool {
	var emptyBound SourceBound
	return b == emptyBound
}

// sourceBoundFromJSON constructs SourceBound from its JSON represent.
func sourceBoundFromJSON(s string) (b SourceBound, err error) {
	if err = json.Unmarshal([]byte(s), &b); err != nil {
		err = terror.ErrHAInvalidItem.Delegate(err, fmt.Sprintf("fail to unmarshal SourceBound %s", s))
	}
	return
}

// PutSourceBound puts the bound relationship into etcd.
// k/v: worker-name -> bound relationship.
func PutSourceBound(cli *clientv3.Client, bounds ...SourceBound) (int64, error) {
	ops := make([]clientv3.Op, 0, len(bounds))
	for _, bound := range bounds {
		boundOps, err := putSourceBoundOp(bound)
		if err != nil {
			return 0, err
		}
		ops = append(ops, boundOps...)
	}
	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(ops...))
	return rev, err
}

// DeleteSourceBound deletes the bound relationship in etcd for the specified worker.
func DeleteSourceBound(cli *clientv3.Client, workers ...string) (int64, error) {
	ops := make([]clientv3.Op, 0, len(workers))
	for _, worker := range workers {
		ops = append(ops, deleteSourceBoundOp(worker)...)
	}
	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(ops...))
	return rev, err
}

// ReplaceSourceBound deletes an old bound and puts a new bound in one transaction, so a bound source will not become
// unbound because of failing halfway.
func ReplaceSourceBound(cli *clientv3.Client, source, oldWorker, newWorker string) (int64, error) {
	deleteOps := deleteSourceBoundOp(oldWorker)
	putOps, err := putSourceBoundOp(NewSourceBound(source, newWorker))
	if err != nil {
		return 0, err
	}
	ops := make([]clientv3.Op, 0, len(deleteOps)+len(putOps))
	ops = append(ops, deleteOps...)
	ops = append(ops, putOps...)
	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(ops...))
	return rev, err
}

// GetSourceBound gets the source bound relationship for the specified DM-worker.
// if the bound relationship for the worker name not exist, return with `err == nil`.
// if the worker name is "", it will return all bound relationships as a map{worker-name: bound}.
// if the worker name is given, it will return a map{worker-name: bound} whose length is 1.
func GetSourceBound(cli *clientv3.Client, worker string) (map[string]SourceBound, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	var (
		sbm  = make(map[string]SourceBound)
		resp *clientv3.GetResponse
		err  error
	)
	failpoint.Inject("FailToGetSourceCfg", func() {
		failpoint.Return(sbm, 0, context.DeadlineExceeded)
	})
	if worker != "" {
		resp, err = cli.Get(ctx, common.UpstreamBoundWorkerKeyAdapter.Encode(worker))
	} else {
		resp, err = cli.Get(ctx, common.UpstreamBoundWorkerKeyAdapter.Path(), clientv3.WithPrefix())
	}

	if err != nil {
		return sbm, 0, terror.ErrHAFailTxnOperation.Delegate(err, "fail to get bound relationship")
	}

	sbm, err = sourceBoundFromResp(worker, resp)
	if err != nil {
		return sbm, 0, err
	}

	return sbm, resp.Header.Revision, nil
}

// GetLastSourceBounds gets all last source bound relationship. Different with GetSourceBound, "last source bound" will
// not be deleted when worker offline.
func GetLastSourceBounds(cli *clientv3.Client) (map[string]SourceBound, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	sbm := make(map[string]SourceBound)
	resp, err := cli.Get(ctx, common.UpstreamLastBoundWorkerKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return sbm, 0, terror.ErrHAFailTxnOperation.Delegate(err, "fail to get last bound relationship")
	}

	sbm, err = sourceBoundFromResp("", resp)
	if err != nil {
		return sbm, 0, err
	}

	return sbm, resp.Header.Revision, nil
}

// GetSourceBoundConfig gets the source bound relationship and relative source config at the same time
// for the specified DM-worker. The index worker **must not be empty**:
// if source bound is empty, will return an empty sourceBound and an empty source config
// if source bound is not empty but sourceConfig is empty, will return an error
// if the source bound is different for over retryNum times, will return an error.
func GetSourceBoundConfig(cli *clientv3.Client, worker string) (SourceBound, *config.SourceConfig, int64, error) {
	var (
		bound    SourceBound
		newBound SourceBound
		cfg      *config.SourceConfig
		ok       bool
		retryNum = defaultGetSourceBoundConfigRetry
	)
	sbm, rev, err := GetSourceBound(cli, worker)
	if err != nil {
		return bound, cfg, 0, err
	}
	if bound, ok = sbm[worker]; !ok {
		return bound, cfg, rev, nil
	}

	for retryCnt := 1; retryCnt <= retryNum; retryCnt++ {
		txnResp, rev2, err2 := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(clientv3.OpGet(common.UpstreamBoundWorkerKeyAdapter.Encode(worker)),
			clientv3.OpGet(common.UpstreamConfigKeyAdapter.Encode(bound.Source))))
		if err2 != nil {
			return bound, cfg, 0, err2
		}

		boundResp := txnResp.Responses[0].GetResponseRange()
		sbm2, err2 := sourceBoundFromResp(worker, (*clientv3.GetResponse)(boundResp))
		if err2 != nil {
			return bound, cfg, 0, err2
		}

		newBound, ok = sbm2[worker]
		// when ok is false, newBound will be empty which means bound for this worker has been deleted in this turn
		// if bound is not empty, we should wait for another turn to make sure bound is really deleted.
		if newBound != bound {
			log.L().Warn("source bound has been changed, will take a retry", zap.Stringer("oldBound", bound),
				zap.Stringer("newBound", newBound), zap.Int("retryTime", retryCnt))
			// if we are about to fail, don't update bound to save the last bound to error
			if retryCnt != retryNum {
				bound = newBound
			}
			select {
			case <-cli.Ctx().Done():
				retryNum = 0 // stop retry
			case <-time.After(retryInterval):
				// retryInterval shouldn't be too long because the longer we wait, bound is more
				// possible to be different from newBound
			}
			continue
		}
		// ok == false and newBound == bound means this bound is truly deleted, we don't need source config anymore
		if !ok {
			return bound, cfg, rev2, nil
		}

		cfgResp := txnResp.Responses[1].GetResponseRange()
		scm, err2 := sourceCfgFromResp(bound.Source, (*clientv3.GetResponse)(cfgResp))
		if err2 != nil {
			return bound, cfg, 0, err2
		}
		cfg, ok = scm[bound.Source]
		// ok == false means we have got source bound but there is no source config, this shouldn't happen
		if !ok {
			// this should not happen.
			return bound, cfg, 0, terror.ErrConfigMissingForBound.Generate(bound)
		}

		return bound, cfg, rev2, nil
	}

	return bound, cfg, 0, terror.ErrMasterBoundChanging.Generate(bound, newBound)
}

// WatchSourceBound watches PUT & DELETE operations for the bound relationship of the specified DM-worker.
// For the DELETE operations, it returns an empty bound relationship.
// nolint:dupl
func WatchSourceBound(ctx context.Context, cli *clientv3.Client, worker string, revision int64, outCh chan<- SourceBound, errCh chan<- error) {
	wCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch := cli.Watch(wCtx, common.UpstreamBoundWorkerKeyAdapter.Encode(worker), clientv3.WithRev(revision))

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
					case errCh <- terror.ErrHAFailWatchEtcd.Delegate(resp.Err(), "watch source bound key canceled"):
					case <-ctx.Done():
					}
				}
				return
			}

			for _, ev := range resp.Events {
				var (
					bound SourceBound
					err   error
				)
				switch ev.Type {
				case mvccpb.PUT:
					bound, err = sourceBoundFromJSON(string(ev.Kv.Value))
				case mvccpb.DELETE:
					bound, err = sourceBoundFromKey(string(ev.Kv.Key))
					bound.IsDeleted = true
				default:
					// this should not happen.
					log.L().Error("unsupported etcd event type", zap.Reflect("kv", ev.Kv), zap.Reflect("type", ev.Type))
					continue
				}
				bound.Revision = ev.Kv.ModRevision

				if err != nil {
					select {
					case errCh <- err:
					case <-ctx.Done():
						return
					}
				} else {
					select {
					case outCh <- bound:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}
}

// sourceBoundFromKey constructs an incomplete bound relationship from an etcd key.
func sourceBoundFromKey(key string) (SourceBound, error) {
	var bound SourceBound
	ks, err := common.UpstreamBoundWorkerKeyAdapter.Decode(key)
	if err != nil {
		return bound, err
	}
	bound.Worker = ks[0]
	return bound, nil
}

func sourceBoundFromResp(worker string, resp *clientv3.GetResponse) (map[string]SourceBound, error) {
	sbm := make(map[string]SourceBound)
	if resp.Count == 0 {
		return sbm, nil
	} else if worker != "" && resp.Count > 1 {
		// this should not happen.
		return sbm, terror.ErrConfigMoreThanOne.Generate(resp.Count, "bound relationship", "worker: "+worker)
	}

	for _, kvs := range resp.Kvs {
		bound, err := sourceBoundFromJSON(string(kvs.Value))
		if err != nil {
			return sbm, err
		}
		bound.Revision = kvs.ModRevision
		sbm[bound.Worker] = bound
	}
	return sbm, nil
}

// deleteSourceBoundOp returns a DELETE etcd operation for the bound relationship of the specified DM-worker.
func deleteSourceBoundOp(worker string) []clientv3.Op {
	return []clientv3.Op{
		clientv3.OpDelete(common.UpstreamBoundWorkerKeyAdapter.Encode(worker)),
	}
}

// deleteLastSourceBoundOp returns a DELETE etcd operation for the last bound relationship of the specified DM-worker.
func deleteLastSourceBoundOp(worker string) clientv3.Op {
	return clientv3.OpDelete(common.UpstreamLastBoundWorkerKeyAdapter.Encode(worker))
}

// putSourceBoundOp returns PUT etcd operations for the bound relationship.
// k/v: worker-name -> bound relationship.
func putSourceBoundOp(bound SourceBound) ([]clientv3.Op, error) {
	value, err := bound.toJSON()
	if err != nil {
		return []clientv3.Op{}, err
	}
	key1 := common.UpstreamBoundWorkerKeyAdapter.Encode(bound.Worker)
	op1 := clientv3.OpPut(key1, value)
	key2 := common.UpstreamLastBoundWorkerKeyAdapter.Encode(bound.Worker)
	op2 := clientv3.OpPut(key2, value)

	return []clientv3.Op{op1, op2}, nil
}
