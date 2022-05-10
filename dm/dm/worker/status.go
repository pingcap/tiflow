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

package worker

import (
	"encoding/json"
	"sort"

	"github.com/gogo/protobuf/jsonpb"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/common"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
)

// Status returns the status of the current sub task.
func (st *SubTask) Status() interface{} {
	if cu := st.CurrUnit(); cu != nil {
		return cu.Status(nil)
	}
	return nil
}

// StatusJSON returns the status of the current sub task as json string.
func (st *SubTask) StatusJSON() string {
	status := st.Status()
	sj, err := json.Marshal(status)
	if err != nil {
		st.l.Error("fail to marshal status", zap.Reflect("status", status), zap.Error(err))
		return ""
	}
	return string(sj)
}

// Status returns the status of the worker (and sub tasks)
// if stName is empty, all sub task's status will be returned.
func (w *SourceWorker) Status(stName string, sourceStatus *binlog.SourceStatus) []*pb.SubTaskStatus {
	sts := w.subTaskHolder.getAllSubTasks()

	if len(sts) == 0 {
		return nil // no sub task started
	}

	status := make([]*pb.SubTaskStatus, 0, len(sts))

	// return status order by name
	names := make([]string, 0, len(sts))
	if len(stName) > 0 {
		names = append(names, stName)
	} else {
		for name := range sts {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	for _, name := range names {
		st, ok := sts[name]
		var stStatus pb.SubTaskStatus
		if !ok {
			stStatus = pb.SubTaskStatus{
				Name:   name,
				Status: &pb.SubTaskStatus_Msg{Msg: common.NoSubTaskMsg(name)},
			}
		} else {
			lockID := ""
			op := st.ShardDDLOperation()
			if op != nil {
				lockID = op.ID
			}
			cu := st.CurrUnit()

			stStatus = pb.SubTaskStatus{
				Name:                name,
				Stage:               st.Stage(),
				Result:              st.Result(),
				UnresolvedDDLLockID: lockID,
			}

			if cu != nil {
				stStatus.Unit = cu.Type()
				// oneof status
				us := cu.Status(sourceStatus)
				switch stStatus.Unit {
				case pb.UnitType_Check:
					stStatus.Status = &pb.SubTaskStatus_Check{Check: us.(*pb.CheckStatus)}
				case pb.UnitType_Dump:
					stStatus.Status = &pb.SubTaskStatus_Dump{Dump: us.(*pb.DumpStatus)}
				case pb.UnitType_Load:
					stStatus.Status = &pb.SubTaskStatus_Load{Load: us.(*pb.LoadStatus)}
				case pb.UnitType_Sync:
					stStatus.Status = &pb.SubTaskStatus_Sync{Sync: us.(*pb.SyncStatus)}
				}
			}
		}
		status = append(status, &stStatus)
	}

	return status
}

// GetUnitAndSourceStatusJSON returns the status of the worker and its unit as json string.
// This function will also cause every unit to print its status to log.
func (w *SourceWorker) GetUnitAndSourceStatusJSON(stName string, sourceStatus *binlog.SourceStatus) string {
	sl := &pb.SubTaskStatusList{Status: w.Status(stName, sourceStatus)}
	mar := jsonpb.Marshaler{EmitDefaults: true, Indent: "    "}
	s, err := mar.MarshalToString(sl)
	if err != nil {
		w.l.Error("fail to marshal status", zap.Any("status", sl), zap.Error(err))
		return ""
	}
	return s
}
