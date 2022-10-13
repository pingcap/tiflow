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

	"github.com/pingcap/tiflow/dm/common"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// Stage represents the running stage for a relay or subtask.
type Stage struct {
	Expect pb.Stage `json:"expect"`         // the expectant stage.
	Source string   `json:"source"`         // the source ID of the upstream.
	Task   string   `json:"task,omitempty"` // the task name for subtask; empty for relay.

	// only used to report to the caller of the watcher, do not marsh it.
	// if it's true, it means the stage has been deleted in etcd.
	IsDeleted bool `json:"-"`
	// record the etcd Revision of this Stage
	Revision int64 `json:"-"`
}

// NewRelayStage creates a new Stage instance for relay.
func NewRelayStage(expect pb.Stage, source string) Stage {
	return newStage(expect, source, "")
}

// NewSubTaskStage creates a new Stage instance for subtask.
func NewSubTaskStage(expect pb.Stage, source, task string) Stage {
	return newStage(expect, source, task)
}

func NewValidatorStage(expect pb.Stage, source, task string) Stage {
	return newStage(expect, source, task)
}

// newStage creates a new Stage instance.
func newStage(expect pb.Stage, source, task string) Stage {
	return Stage{
		Expect: expect,
		Source: source,
		Task:   task,
	}
}

// String implements Stringer interface.
func (s Stage) String() string {
	str, _ := s.toJSON()
	return str
}

// toJSON returns the string of JSON represent.
func (s Stage) toJSON() (string, error) {
	data, err := json.Marshal(s)
	if err != nil {
		return "", terror.ErrHAInvalidItem.Delegate(err, fmt.Sprintf("failed to marshal stage %+v", s))
	}
	return string(data), nil
}

// IsEmpty returns true when this Stage has no value.
func (s Stage) IsEmpty() bool {
	var emptyStage Stage
	return s == emptyStage
}

// stageFromJSON constructs Stage from its JSON represent.
func stageFromJSON(str string) (s Stage, err error) {
	if err = json.Unmarshal([]byte(str), &s); err != nil {
		err = terror.ErrHAInvalidItem.Delegate(err, fmt.Sprintf("failed to unmarshal stage %s", str))
	}
	return
}

// PutRelayStage puts the stage of the relay into etcd.
// k/v: sourceID -> the running stage of the relay.
func PutRelayStage(cli *clientv3.Client, stages ...Stage) (int64, error) {
	ops, err := putRelayStageOp(stages...)
	if err != nil {
		return 0, err
	}
	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(ops...))
	return rev, err
}

// DeleteRelayStage deleted the relay stage of this source.
func DeleteRelayStage(cli *clientv3.Client, source string) (int64, error) {
	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(deleteRelayStageOp(source)))
	return rev, err
}

// PutSubTaskStage puts the stage of the subtask into etcd.
// k/v: sourceID, task -> the running stage of the subtask.
func PutSubTaskStage(cli *clientv3.Client, stages ...Stage) (int64, error) {
	ops, err := putSubTaskStageOp(stages...)
	if err != nil {
		return 0, err
	}
	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(ops...))
	return rev, err
}

// GetRelayStage gets the relay stage for the specified upstream source.
// if the stage for the source not exist, return with `err == nil` and `revision=0`.
func GetRelayStage(cli *clientv3.Client, source string) (Stage, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	var stage Stage
	resp, err := cli.Get(ctx, common.StageRelayKeyAdapter.Encode(source))
	if err != nil {
		return stage, 0, terror.ErrHAFailTxnOperation.Delegate(err, "failed to get relay stage for source %s", source)
	}

	if resp.Count == 0 {
		return stage, resp.Header.Revision, nil
	} else if resp.Count > 1 {
		// this should not happen.
		return stage, 0, terror.ErrConfigMoreThanOne.Generate(resp.Count, "relay stage", "source: "+source)
	}

	stage, err = stageFromJSON(string(resp.Kvs[0].Value))
	if err != nil {
		return stage, 0, err
	}
	stage.Revision = resp.Kvs[0].ModRevision

	return stage, resp.Header.Revision, nil
}

// GetAllRelayStage gets all relay stages.
// k/v: source ID -> relay stage.
func GetAllRelayStage(cli *clientv3.Client) (map[string]Stage, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Get(ctx, common.StageRelayKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return nil, 0, terror.ErrHAFailTxnOperation.Delegate(err, "failed to get all relay stages")
	}

	stages := make(map[string]Stage)
	for _, kv := range resp.Kvs {
		stage, err2 := stageFromJSON(string(kv.Value))
		if err2 != nil {
			return nil, 0, err2
		}
		stage.Revision = kv.ModRevision
		stages[stage.Source] = stage
	}
	return stages, resp.Header.Revision, nil
}

// GetSubTaskStage gets the subtask stage for the specified upstream source and task name.
// if the stage for the source and task name not exist, return with `err == nil` and `revision=0`.
// if task name is "", it will return all subtasks' stage as a map{task-name: stage} for the source.
// if task name is given, it will return a map{task-name: stage} whose length is 1.
func GetSubTaskStage(cli *clientv3.Client, source, task string) (map[string]Stage, int64, error) {
	return getStageByKey(cli, common.StageSubTaskKeyAdapter, source, task, 0)
}

func getStageByKey(cli *clientv3.Client, key common.KeyAdapter, source, task string, revision int64) (map[string]Stage, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	var (
		stm  = make(map[string]Stage)
		resp *clientv3.GetResponse
		err  error
		opts = make([]clientv3.OpOption, 0)
	)
	if revision > 0 {
		opts = append(opts, clientv3.WithRev(revision))
	}
	if task != "" {
		resp, err = cli.Get(ctx, key.Encode(source, task), opts...)
	} else {
		opts = append(opts, clientv3.WithPrefix())
		resp, err = cli.Get(ctx, key.Encode(source), opts...)
	}

	if err != nil {
		return stm, 0, terror.ErrHAFailTxnOperation.Delegate(err, "failed to get subtask stage for source %s, task %s", source, task)
	}

	stages, err := getStagesFromResp(source, task, resp)
	if err != nil {
		return stm, 0, err
	}
	stm = stages[source]

	return stm, resp.Header.Revision, nil
}

func GetValidatorStage(cli *clientv3.Client, source, task string, revision int64) (map[string]Stage, int64, error) {
	return getStageByKey(cli, common.StageValidatorKeyAdapter, source, task, revision)
}

// GetAllSubTaskStage gets all subtask stages.
// k/v: source ID -> task name -> subtask stage.
func GetAllSubTaskStage(cli *clientv3.Client) (map[string]map[string]Stage, int64, error) {
	return getAllStagesInner(cli, common.StageSubTaskKeyAdapter)
}

func getAllStagesInner(cli *clientv3.Client, key common.KeyAdapter) (map[string]map[string]Stage, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Get(ctx, key.Path(), clientv3.WithPrefix())
	if err != nil {
		return nil, 0, terror.ErrHAFailTxnOperation.Delegate(err, "failed to get all subtask stages")
	}

	stages, err := getStagesFromResp("", "", resp)
	if err != nil {
		return nil, 0, err
	}

	return stages, resp.Header.Revision, nil
}

func GetAllValidatorStage(cli *clientv3.Client) (map[string]map[string]Stage, int64, error) {
	return getAllStagesInner(cli, common.StageValidatorKeyAdapter)
}

// GetSubTaskStageConfig gets source's subtask stages and configs at the same time
// source **must not be empty**
// return map{task name -> subtask stage}, map{task name -> validator stage}, map{task name -> subtask config}, revision, error.
func GetSubTaskStageConfig(cli *clientv3.Client, source string) (map[string]Stage, map[string]Stage, map[string]config.SubTaskConfig, int64, error) {
	var (
		stm               = make(map[string]Stage)
		validatorStageMap = make(map[string]Stage)
		scm               = make(map[string]config.SubTaskConfig)
	)
	txnResp, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(
		clientv3.OpGet(common.StageSubTaskKeyAdapter.Encode(source), clientv3.WithPrefix()),
		clientv3.OpGet(common.StageValidatorKeyAdapter.Encode(source), clientv3.WithPrefix()),
		clientv3.OpGet(common.UpstreamSubTaskKeyAdapter.Encode(source), clientv3.WithPrefix())))
	if err != nil {
		return stm, validatorStageMap, scm, 0, err
	}
	stageResp := txnResp.Responses[0].GetResponseRange()
	stages, err := getStagesFromResp(source, "", (*clientv3.GetResponse)(stageResp))
	if err != nil {
		return stm, validatorStageMap, scm, 0, err
	}
	stm = stages[source]

	validatorStageResp := txnResp.Responses[1].GetResponseRange()
	validatorStages, err := getStagesFromResp(source, "", (*clientv3.GetResponse)(validatorStageResp))
	if err != nil {
		return stm, validatorStageMap, scm, 0, err
	}
	validatorStageMap = validatorStages[source]

	cfgResp := txnResp.Responses[2].GetResponseRange()
	cfgs, err := subTaskCfgFromResp(source, "", (*clientv3.GetResponse)(cfgResp))
	if err != nil {
		return stm, validatorStageMap, scm, 0, err
	}
	scm = cfgs[source]

	return stm, validatorStageMap, scm, rev, err
}

// WatchRelayStage watches PUT & DELETE operations for the relay stage.
// for the DELETE stage, it returns an empty stage.
func WatchRelayStage(ctx context.Context, cli *clientv3.Client,
	source string, revision int64, outCh chan<- Stage, errCh chan<- error,
) {
	wCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch := cli.Watch(wCtx, common.StageRelayKeyAdapter.Encode(source), clientv3.WithRev(revision))
	watchStage(ctx, ch, relayStageFromKey, outCh, errCh)
}

// WatchSubTaskStage watches PUT & DELETE operations for the subtask stage.
// for the DELETE stage, it returns an empty stage.
func WatchSubTaskStage(ctx context.Context, cli *clientv3.Client,
	source string, revision int64, outCh chan<- Stage, errCh chan<- error,
) {
	wCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch := cli.Watch(wCtx, common.StageSubTaskKeyAdapter.Encode(source), clientv3.WithPrefix(), clientv3.WithRev(revision))
	watchStage(ctx, ch, subTaskStageFromKey, outCh, errCh)
}

func WatchValidatorStage(ctx context.Context, cli *clientv3.Client,
	source string, rev int64, outCh chan<- Stage, errCh chan<- error,
) {
	wCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch := cli.Watch(wCtx, common.StageValidatorKeyAdapter.Encode(source), clientv3.WithPrefix(), clientv3.WithRev(rev))
	watchStage(ctx, ch, validatorStageFromKey, outCh, errCh)
}

// DeleteSubTaskStage deletes the subtask stage.
func DeleteSubTaskStage(cli *clientv3.Client, stages ...Stage) (int64, error) {
	ops := deleteSubTaskStageOp(stages...)
	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(ops...))
	return rev, err
}

// relayStageFromKey constructs an incomplete relay stage from an etcd key.
func relayStageFromKey(key string) (Stage, error) {
	var stage Stage
	ks, err := common.StageRelayKeyAdapter.Decode(key)
	if err != nil {
		return stage, err
	}
	stage.Source = ks[0]
	return stage, nil
}

// subTaskStageFromKey constructs an incomplete subtask stage from an etcd key.
func subTaskStageFromKey(key string) (Stage, error) {
	var stage Stage
	ks, err := common.StageSubTaskKeyAdapter.Decode(key)
	if err != nil {
		return stage, err
	}
	stage.Source = ks[0]
	stage.Task = ks[1]
	return stage, nil
}

func validatorStageFromKey(key string) (Stage, error) {
	var stage Stage
	ks, err := common.StageValidatorKeyAdapter.Decode(key)
	if err != nil {
		return stage, err
	}
	stage.Source = ks[0]
	stage.Task = ks[1]
	return stage, nil
}

func getStagesFromResp(source, task string, resp *clientv3.GetResponse) (map[string]map[string]Stage, error) {
	stages := make(map[string]map[string]Stage)
	if source != "" {
		stages[source] = make(map[string]Stage) // avoid stages[source] is nil
	}

	if resp.Count == 0 {
		return stages, nil
	} else if source != "" && task != "" && resp.Count > 1 {
		// this should not happen.
		return stages, terror.ErrConfigMoreThanOne.Generate(resp.Count, "stage", "(source "+source+", task "+task+")")
	}

	for _, kvs := range resp.Kvs {
		stage, err := stageFromJSON(string(kvs.Value))
		if err != nil {
			return nil, err
		}
		if _, ok := stages[stage.Source]; !ok {
			stages[stage.Source] = make(map[string]Stage)
		}
		stage.Revision = kvs.ModRevision
		stages[stage.Source][stage.Task] = stage
	}
	return stages, nil
}

// watchStage watches PUT & DELETE operations for the stage.
// nolint:dupl
func watchStage(ctx context.Context, watchCh clientv3.WatchChan,
	stageFromKey func(key string) (Stage, error), outCh chan<- Stage, errCh chan<- error,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case resp, ok := <-watchCh:
			if !ok {
				return
			}
			if resp.Canceled {
				// TODO(csuzhangxc): do retry here.
				if resp.Err() != nil {
					select {
					case errCh <- terror.ErrHAFailWatchEtcd.Delegate(resp.Err(), "watch stage canceled"):
					case <-ctx.Done():
					}
				}
				return
			}

			for _, ev := range resp.Events {
				var (
					stage Stage
					err   error
				)
				switch ev.Type {
				case mvccpb.PUT:
					stage, err = stageFromJSON(string(ev.Kv.Value))
				case mvccpb.DELETE:
					stage, err = stageFromKey(string(ev.Kv.Key))
					stage.IsDeleted = true
				default:
					// this should not happen.
					log.L().Error("unsupported etcd event type", zap.Reflect("kv", ev.Kv), zap.Reflect("type", ev.Type))
					continue
				}
				stage.Revision = ev.Kv.ModRevision

				if err != nil {
					select {
					case errCh <- err:
					case <-ctx.Done():
						return
					}
				} else {
					select {
					case outCh <- stage:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}
}

// putRelayStageOp returns a list of PUT etcd operation for the relay stage.
// k/v: sourceID -> the running stage of the relay.
func putRelayStageOp(stages ...Stage) ([]clientv3.Op, error) {
	ops := make([]clientv3.Op, 0, len(stages))
	for _, stage := range stages {
		value, err := stage.toJSON()
		if err != nil {
			return ops, err
		}
		key := common.StageRelayKeyAdapter.Encode(stage.Source)
		ops = append(ops, clientv3.OpPut(key, value))
	}
	return ops, nil
}

// putSubTaskStageOp returns a list of PUT etcd operations for the subtask stage.
// k/v: sourceID, task -> the running stage of the subtask.
func putSubTaskStageOp(stages ...Stage) ([]clientv3.Op, error) {
	ops := make([]clientv3.Op, 0, len(stages))
	for _, stage := range stages {
		value, err := stage.toJSON()
		if err != nil {
			return ops, err
		}
		key := common.StageSubTaskKeyAdapter.Encode(stage.Source, stage.Task)
		ops = append(ops, clientv3.OpPut(key, value))
	}
	return ops, nil
}

// deleteRelayStageOp returns a DELETE etcd operation for the relay stage.
func deleteRelayStageOp(source string) clientv3.Op {
	return clientv3.OpDelete(common.StageRelayKeyAdapter.Encode(source))
}

// deleteSubTaskStageOp returns a list of DELETE etcd operation for the subtask stage.
func deleteSubTaskStageOp(stages ...Stage) []clientv3.Op {
	ops := make([]clientv3.Op, 0, len(stages))
	for _, stage := range stages {
		ops = append(ops, clientv3.OpDelete(common.StageSubTaskKeyAdapter.Encode(stage.Source, stage.Task)))
	}
	return ops
}
