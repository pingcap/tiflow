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

package canal

import (
	"github.com/pingcap/ticdc/integration/framework"
)

// SingleTableTask provides a basic implementation for an Avro test case
type SingleTableTask struct {
	*framework.BaseSingleTableTask
	UseJSON bool
}

// NewSingleTableTask return a pointer of SingleTableTask
func NewSingleTableTask(tableName string, useJSON bool) *SingleTableTask {
	return &SingleTableTask{
		BaseSingleTableTask: framework.NewBaseSingleTableTask(tableName),
		UseJSON:             useJSON,
	}
}

// GetCDCProfile implements Task
func (c *SingleTableTask) GetCDCProfile() *framework.CDCProfile {
	var protocol string
	if c.UseJSON {
		protocol = "canal-json"
	} else {
		protocol = "canal"
	}
	return &framework.CDCProfile{
		PDUri:      "http://upstream-pd:2379",
		SinkURI:    "kafka://kafka:9092/" + framework.TestDbName + "?kafka-version=2.6.0&protocol=" + protocol,
		Opts:       map[string]string{"force-handle-key-pkey": "true", "support-txn": "true"},
		ConfigFile: "/config/canal-test-config.toml",
	}
}
