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
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/integration/framework"
)

// SingleTableTask provides a basic implementation for an Avro test case
type SingleTableTask struct {
	*framework.BaseSingleTableTask
	CheckOldValue bool
}

// NewSingleTableTask return a pointer of SingleTableTask
func NewSingleTableTask(tableName string, checkOld bool) *SingleTableTask {
	return &SingleTableTask{
		BaseSingleTableTask: framework.NewBaseSingleTableTask(tableName),
		CheckOldValue:       checkOld,
	}
}

// GetCDCProfile implements Task
func (c *SingleTableTask) GetCDCProfile() *framework.CDCProfile {
	sinkURI := "mysql://downstream-tidb:4000/" + framework.TestDbName
	if c.CheckOldValue {
		sinkURI = "simple-mysql://downstream-tidb:4000/" + framework.TestDbName + "?check-old-value=true"
	}
	return &framework.CDCProfile{
		PDUri:      "http://upstream-pd:2379",
		SinkURI:    sinkURI,
		Opts:       map[string]string{},
		ConfigFile: "/config/enable-oldvalue-config.toml",
	}
}

// Run implements Task
func (c *SingleTableTask) Run(taskContext *framework.TaskContext) error {
	log.Warn("SingleTableTask has been run")
	return nil
}
