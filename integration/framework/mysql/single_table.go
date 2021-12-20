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

package mysql

import (
	"database/sql"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/integration/framework"
)

const (
	testDbName = "testdb"
)

// SingleTableTask provides a basic implementation for an Avro test case
type SingleTableTask struct {
	TableName     string
	CheckOleValue bool
}

// Name implements Task
func (c *SingleTableTask) Name() string {
	log.Warn("SingleTableTask should be embedded in another Task")
	return "SingleTableTask-" + c.TableName
}

// GetCDCProfile implements Task
func (c *SingleTableTask) GetCDCProfile() *framework.CDCProfile {
	sinkURI := "mysql://downstream-tidb:4000/" + testDbName
	if c.CheckOleValue {
		sinkURI = "simple-mysql://downstream-tidb:4000/" + testDbName + "?check-old-value=true"
	}
	return &framework.CDCProfile{
		PDUri:      "http://upstream-pd:2379",
		SinkURI:    sinkURI,
		Opts:       map[string]string{},
		ConfigFile: "/config/enable-oldvalue-config.toml",
	}
}

// Prepare implements Task
func (c *SingleTableTask) Prepare(taskContext *framework.TaskContext) error {
	err := taskContext.CreateDB(testDbName)
	if err != nil {
		return err
	}

	_ = taskContext.Upstream.Close()
	taskContext.Upstream, err = sql.Open("mysql", framework.UpstreamDSN+testDbName)
	if err != nil {
		return err
	}

	_ = taskContext.Downstream.Close()
	taskContext.Downstream, err = sql.Open("mysql", framework.DownstreamDSN+testDbName)
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
func (c *SingleTableTask) Run(taskContext *framework.TaskContext) error {
	log.Warn("SingleTableTask has been run")
	return nil
}
