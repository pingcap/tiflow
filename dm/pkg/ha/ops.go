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
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/etcdutil"
)

// PutRelayStageRelayConfigSourceBound puts the following data in one txn.
// - relay stage.
// - relay config for a worker
// - source bound relationship.
func PutRelayStageRelayConfigSourceBound(cli *clientv3.Client, stage Stage, bound SourceBound) (int64, error) {
	ops1, err := putRelayStageOp(stage)
	if err != nil {
		return 0, err
	}
	op2, err := putSourceBoundOp(bound)
	if err != nil {
		return 0, err
	}
	op3 := putRelayConfigOp(bound.Worker, bound.Source)
	ops := make([]clientv3.Op, 0, len(ops1)+len(op2)+1)
	ops = append(ops, ops1...)
	ops = append(ops, op2...)
	ops = append(ops, op3)
	_, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, ops...)
	return rev, err
}

// DeleteSourceCfgRelayStageSourceBound deletes the following data in one txn.
// - upstream source config.
// - relay stage.
// - source bound relationship.
func DeleteSourceCfgRelayStageSourceBound(cli *clientv3.Client, source, worker string) (int64, error) {
	sourceCfgOp := deleteSourceCfgOp(source)
	relayStageOp := deleteRelayStageOp(source)
	sourceBoundOp := deleteSourceBoundOp(worker)
	lastBoundOp := deleteLastSourceBoundOp(worker)
	ops := make([]clientv3.Op, 0, 3+len(sourceBoundOp))
	ops = append(ops, sourceCfgOp)
	ops = append(ops, relayStageOp)
	ops = append(ops, sourceBoundOp...)
	ops = append(ops, lastBoundOp)

	_, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, ops...)
	return rev, err
}

// PutSubTaskCfgStage puts the following data in one txn.
// - subtask config.
// - subtask stage.
// NOTE: golang can't use two `...` in the func, so use `[]` instead.
func PutSubTaskCfgStage(cli *clientv3.Client, cfgs []config.SubTaskConfig, stages []Stage) (int64, error) {
	return opSubTaskCfgStage(cli, mvccpb.PUT, cfgs, stages)
}

// DeleteSubTaskCfgStage deletes the following data in one txn.
// - subtask config.
// - subtask stage.
// NOTE: golang can't use two `...` in the func, so use `[]` instead.
func DeleteSubTaskCfgStage(cli *clientv3.Client, cfgs []config.SubTaskConfig, stages []Stage) (int64, error) {
	return opSubTaskCfgStage(cli, mvccpb.DELETE, cfgs, stages)
}

// opSubTaskCfgStage puts/deletes for subtask config and stage in one txn.
func opSubTaskCfgStage(cli *clientv3.Client, evType mvccpb.Event_EventType,
	cfgs []config.SubTaskConfig, stages []Stage) (int64, error) {
	var (
		ops1 []clientv3.Op
		ops2 []clientv3.Op
		err  error
	)
	switch evType {
	case mvccpb.PUT:
		ops1, err = putSubTaskCfgOp(cfgs...)
		if err != nil {
			return 0, err
		}
		ops2, err = putSubTaskStageOp(stages...)
		if err != nil {
			return 0, err
		}
	case mvccpb.DELETE:
		ops1 = deleteSubTaskCfgOp(cfgs...)
		ops2 = deleteSubTaskStageOp(stages...)
	}

	ops := make([]clientv3.Op, 0, len(ops1)+len(ops2))
	ops = append(ops, ops1...)
	ops = append(ops, ops2...)
	_, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, ops...)
	return rev, err
}
