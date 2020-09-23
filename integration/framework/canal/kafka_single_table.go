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
	"database/sql"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/integration/framework"
	"go.uber.org/zap"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/pingcap/log"
)

const (
	testDbName = "testdb"
)

// SingleTableTask provides a basic implementation for an Avro test case
type SingleTableTask struct {
	TableName string
}

// Name implements Task
func (c *SingleTableTask) Name() string {
	log.Warn("SingleTableTask should be embedded in another Task")
	return "SingleTableTask-" + c.TableName
}

// GetCDCProfile implements Task
func (c *SingleTableTask) GetCDCProfile() *framework.CDCProfile {
	return &framework.CDCProfile{
		PDUri:      "http://upstream-pd:2379",
		SinkURI:    "kafka://kafka:9092/" + testDbName + "?protocol=canal",
		Opts:       map[string]string{"force-handle-key-pkey": "true"},
		ConfigFile: "/config/canal-test-config.toml",
	}
}

// Prepare implements Task
func (c *SingleTableTask) Prepare(taskContext *framework.TaskContext) error {
	err := taskContext.CreateDB(testDbName)
	if err != nil {
		return err
	}

	_ = taskContext.Upstream.Close()
	taskContext.Upstream, err = sql.Open("mysql", upstreamDSN+testDbName)
	if err != nil {
		return err
	}

	_ = taskContext.Downstream.Close()
	taskContext.Downstream, err = sql.Open("mysql", downstreamDSN+testDbName)
	if err != nil {
		return err
	}
	taskContext.Downstream.SetConnMaxLifetime(5 * time.Second)

	if err = c.checkCanalAdapterState(); err != nil {
		return err
	}
	if taskContext.WaitForReady != nil {
		log.Info("Waiting for env to be ready")
		return taskContext.WaitForReady()
	}
	return nil
}

func (c *SingleTableTask) checkCanalAdapterState() error {
	resp, err := http.Get(
		"http://127.0.0.1:8081/syncSwitch/" + testDbName)
	if err != nil {
		return err
	}

	if resp.Body == nil {
		return errors.New("Canal Adapter Rest API returned empty body, there is no subscript topic")
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		str, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		log.Warn(
			"Canal Adapter Rest API returned",
			zap.Int("status", resp.StatusCode),
			zap.ByteString("body", str))
		return errors.Errorf("Kafka Connect Rest API returned status code %d", resp.StatusCode)
	}
	return nil
}

// Run implements Task
func (c *SingleTableTask) Run(taskContext *framework.TaskContext) error {
	log.Warn("SingleTableTask has been run")
	return nil
}
