// Copyright 2022 PingCAP, Inc.
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

package master

import (
	"context"

	"github.com/pingcap/tiflow/dm/checker"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

func (s *Server) checkTask(ctx context.Context, subtaskCfgList []*config.SubTaskConfig, errCnt, warnCnt int64) (string, error) {
	return checker.CheckSyncConfigFunc(ctx, subtaskCfgList, errCnt, warnCnt)
}

func (s *Server) createTask(ctx context.Context, subtaskCfgList []*config.SubTaskConfig) error {
	return s.scheduler.AddSubTasks(false, pb.Stage_Stopped, subtaskCfgPointersToInstances(subtaskCfgList...)...)
}

// nolint:unused
func (s *Server) updateTask(ctx context.Context, taskCfg *config.TaskConfig) error {
	// TODO(ehco) no caller now , will implement later
	return nil
}

// nolint:unused
func (s *Server) deleteTask(ctx context.Context, taskCfg *config.TaskConfig) error {
	// TODO(ehco) no caller now , will implement later
	return nil
}

func (s *Server) getTask(ctx context.Context, req interface{}) error {
	// TODO(ehco) no caller now , will implement later
	return nil
}

func (s *Server) getTaskStatus(ctx context.Context, req interface{}) error {
	// TODO(ehco) no caller now , will implement later
	return nil
}

func (s *Server) listTask(ctx context.Context, req interface{}) error {
	// TODO(ehco) no caller now , will implement later
	return nil
}

func (s *Server) startTask(ctx context.Context, taskName string, sourceNameList []string, remoteMeta bool, req interface{}) error {
	subTaskConfigM := s.scheduler.GetSubTaskCfgsByTask(taskName)
	var needStartSubTaskList []*config.SubTaskConfig
	for _, sourceName := range sourceNameList {
		subTaskCfg, ok := subTaskConfigM[sourceName]
		if !ok {
			return terror.ErrSchedulerSourceCfgNotExist.Generate(sourceName)
		}
		needStartSubTaskList = append(needStartSubTaskList, subTaskCfg)
	}
	if len(needStartSubTaskList) == 0 {
		return nil
	}
	if remoteMeta {
		// use same latch for remove-meta and start-task
		release, err := s.scheduler.AcquireSubtaskLatch(taskName)
		if err != nil {
			return terror.ErrSchedulerLatchInUse.Generate("RemoveMeta", taskName)
		}
		defer release()
		metaSchema := needStartSubTaskList[0].MetaSchema
		targetDB := needStartSubTaskList[0].To
		err = s.removeMetaData(ctx, taskName, metaSchema, &targetDB)
		if err != nil {
			return terror.Annotate(err, "while removing metadata")
		}
		release()
	}
	return s.scheduler.UpdateExpectSubTaskStage(pb.Stage_Running, taskName, sourceNameList...)
}

// nolint:unused
func (s *Server) stopTask(ctx context.Context, req interface{}) error {
	// TODO(ehco) no caller now , will implement later
	return nil
}
