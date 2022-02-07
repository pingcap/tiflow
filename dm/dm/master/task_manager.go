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

func generateTaskConfig(ctx context.Context, taskYamlString string, cliArgs *config.TaskCliArgs) (*config.TaskConfig, error) {
	cfg := config.NewTaskConfig()
	// bypass the meta check by set any value. If start-time is specified, DM-worker will not use meta field.
	if cliArgs != nil && cliArgs.StartTime != "" {
		for _, inst := range cfg.MySQLInstances {
			inst.Meta = &config.Meta{BinLogName: cliArgs.StartTime}
		}
	}
	err := cfg.Decode(taskYamlString)
	if err != nil {
		return nil, terror.WithClass(err, terror.ClassDMMaster)
	}
	err = adjustTargetDB(ctx, cfg.TargetDB)
	if err != nil {
		return nil, terror.WithClass(err, terror.ClassDMMaster)
	}
	return cfg, nil
}

func (s *Server) generateSubTaskConfigs(taskCfg *config.TaskConfig) ([]*config.SubTaskConfig, error) {
	sourceCfgs := s.getSourceConfigs(taskCfg.MySQLInstances)
	subtaskCfgList, err := config.TaskConfigToSubTaskConfigs(taskCfg, sourceCfgs)
	if err != nil {
		return nil, terror.WithClass(err, terror.ClassDMMaster)
	}
	return subtaskCfgList, nil
}

// checkTask checks legality of task configuration.
func (s *Server) checkTask(ctx context.Context, taskCfg *config.TaskConfig, errCnt, warnCnt int64) (string, error) {
	subtaskCfgList, err := s.generateSubTaskConfigs(taskCfg)
	if err != nil {
		return "", err
	}
	return checker.CheckSyncConfigFunc(ctx, subtaskCfgList, errCnt, warnCnt)
}

// createTask convert task to subtasks and put these subtasks with stopped stage to etcd.
// all configs will be checked when user call `s.StartTask`.
func (s *Server) createTask(ctx context.Context, taskCfg *config.TaskConfig) error {
	subtaskCfgList, err := s.generateSubTaskConfigs(taskCfg)
	if err != nil {
		return err
	}
	return s.scheduler.AddSubTasks(false, pb.Stage_Stopped, subtaskCfgPointersToInstances(subtaskCfgList...)...)
}

func (s *Server) updateTask(ctx context.Context, taskCfg *config.TaskConfig) error {
	// TODO(ehco) no caller now , will implement later
	return nil
}

func (s *Server) deleteTask(ctx context.Context, taskCfg *config.TaskConfig) error {
	// TODO(ehco) no caller now , will implement later
	return nil
}

func (s *Server) getTask(ctx context.Context, req interface{}) error {
	// TODO(ehco) no caller now , will implement later
	return nil
}

func (s *Server) listTask(ctx context.Context, req interface{}) error {
	// TODO(ehco) no caller now , will implement later
	return nil
}

func (s *Server) listTaskStats(ctx context.Context, req interface{}) error {
	// TODO(ehco) no caller now , will implement later
	return nil
}

func (s *Server) startTask(ctx context.Context, req interface{}) error {
	// TODO(ehco) no caller now , will implement later
	return nil
}

func (s *Server) stopTask(ctx context.Context, req interface{}) error {
	// TODO(ehco) no caller now , will implement later
	return nil
}
