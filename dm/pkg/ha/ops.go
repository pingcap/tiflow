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
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// PutRelayStageSourceBound puts the following data in one txn.
// - relay stage.
// - source bound relationship.
func PutRelayStageSourceBound(cli *clientv3.Client, stage Stage, bound SourceBound) (int64, error) {
	ops1, err := putRelayStageOp(stage)
	if err != nil {
		return 0, err
	}
	op2, err := putSourceBoundOp(bound)
	if err != nil {
		return 0, err
	}
	ops := make([]clientv3.Op, 0, len(ops1)+len(op2))
	ops = append(ops, ops1...)
	ops = append(ops, op2...)
	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(ops...))
	return rev, err
}

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
	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(ops...))
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

	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(ops...))
	return rev, err
}

// PutSubTaskCfgStage puts the following data in one txn.
// - subtask config.
// - subtask stage.
// NOTE: golang can't use two `...` in the func, so use `[]` instead.
func PutSubTaskCfgStage(cli *clientv3.Client, cfgs []config.SubTaskConfig, stages []Stage, validatorStages []Stage) (int64, error) {
	return operateSubtask(cli, mvccpb.PUT, cfgs, stages, validatorStages)
}

// DeleteSubTaskCfgStage deletes the following data in one txn.
// - subtask config.
// - subtask stage.
// NOTE: golang can't use two `...` in the func, so use `[]` instead.
func DeleteSubTaskCfgStage(cli *clientv3.Client, cfgs []config.SubTaskConfig, stages []Stage, validatorStages []Stage) (int64, error) {
	return operateSubtask(cli, mvccpb.DELETE, cfgs, stages, validatorStages)
}

// operateSubtask puts/deletes KVs for the subtask in one txn.
func operateSubtask(cli *clientv3.Client, evType mvccpb.Event_EventType, cfgs []config.SubTaskConfig, stages []Stage,
	validatorStages []Stage,
) (int64, error) {
	var (
		ops1         []clientv3.Op
		ops2         []clientv3.Op
		validatorOps []clientv3.Op
		err          error
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
		validatorOps, err = putValidatorStageOps(validatorStages...)
		if err != nil {
			return 0, err
		}
	case mvccpb.DELETE:
		ops1 = deleteSubTaskCfgOp(cfgs...)
		ops2 = deleteSubTaskStageOp(stages...)
		validatorOps = deleteValidatorStageOps(validatorStages...)
	}

	ops := make([]clientv3.Op, 0, 2*len(cfgs)+len(stages))
	ops = append(ops, ops1...)
	ops = append(ops, ops2...)
	ops = append(ops, validatorOps...)
	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(ops...))
	return rev, err
}
