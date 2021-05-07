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

package framework

import (
	"database/sql"
	"time"

	"github.com/pingcap/log"
)

const (
	// TestDbName referenced by Task that embed BaseSingleTableTask
	TestDbName = "testdb"
)

// BaseSingleTableTask should be embedded into implementation of SingleTableTask
type BaseSingleTableTask struct {
	TableName string
}

// NewBaseSingleTableTask return a pointer of BaseSingleTableTask
func NewBaseSingleTableTask(name string) *BaseSingleTableTask {
	return &BaseSingleTableTask{TableName: name}
}

// Name implements Task
func (t *BaseSingleTableTask) Name() string {
	log.Warn("SingleTableTask should be embedded in another Task")
	return "SingleTableTask-" + t.TableName
}

// Prepare implements Task
func (t *BaseSingleTableTask) Prepare(taskContext *TaskContext) error {
	err := taskContext.CreateDB(TestDbName)
	if err != nil {
		return err
	}

	_ = taskContext.Upstream.Close()
	taskContext.Upstream, err = sql.Open("mysql", UpstreamDSN+TestDbName)
	if err != nil {
		return err
	}

	_ = taskContext.Downstream.Close()
	taskContext.Downstream, err = sql.Open("mysql", DownstreamDSN+TestDbName)
	if err != nil {
		return err
	}
	taskContext.Downstream.SetConnMaxLifetime(5 * time.Second)
	if taskContext.WaitForReady != nil {
		log.Info("Waiting for env to be ready")
		return taskContext.WaitForReady()
	}

	return nil
}

// Run implements Task
func (t *BaseSingleTableTask) Run(taskContext *TaskContext) error {
	log.Warn("SingleTableTask has been run")
	return nil
}
