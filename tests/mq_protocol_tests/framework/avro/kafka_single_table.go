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

package avro

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/tests/mq_protocol_tests/framework"
	"go.uber.org/zap"
)

// SingleTableTask provides a basic implementation for an Avro test case
type SingleTableTask struct {
	TableName string
}

// Name implements Task
func (a *SingleTableTask) Name() string {
	log.Warn("SingleTableTask should be embedded in another Task")
	return "SingleTableTask-" + a.TableName
}

// GetCDCProfile implements Task
func (a *SingleTableTask) GetCDCProfile() *framework.CDCProfile {
	return &framework.CDCProfile{
		PDUri: framework.UpstreamPD,
		SinkURI: "kafka://kafka:9092/testdb_" + a.TableName +
			"?kafka-version=2.6.0&protocol=avro&avro-bigint-unsigned-handling-mode=string",
		SchemaRegistry: "http://schema-registry:8081",
	}
}

// Prepare implements Task
func (a *SingleTableTask) Prepare(taskContext *framework.TaskContext) error {
	err := taskContext.CreateDB("testdb")
	if err != nil {
		return err
	}

	_ = taskContext.Upstream.Close()
	taskContext.Upstream, err = sql.Open("mysql", framework.UpstreamDSN+"testdb")
	if err != nil {
		return err
	}

	_ = taskContext.Downstream.Close()
	taskContext.Downstream, err = sql.Open("mysql", framework.DownstreamDSN+"testdb")
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
func (a *SingleTableTask) Run(taskContext *framework.TaskContext) error {
	log.Warn("SingleTableTask has been run")
	return nil
}

func createConnector() error {
	// TODO better way to generate JSON
	connectorConfigFmt := `{
		"name": "jdbc-sink-connector-debug",
		"config": {
		  "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
		  "tasks.max": "1",
		  "topics": "testdb_%s",
		  "connection.url": "jdbc:mysql://root@downstream-tidb:4000/testdb",
		  "connection.ds.pool.size": 5,
		  "table.name.format": "%s",
		  "insert.mode": "upsert",
		  "delete.enabled": true,
		  "pk.mode": "record_key",
		  "auto.create": true,
		  "auto.evolve": true
		}
	  }`
	connectorConfig := fmt.Sprintf(connectorConfigFmt, "test", "test")
	log.Debug("Creating Kafka sink connector", zap.String("config", connectorConfig))

	resp, err := http.Post(
		"http://127.0.0.1:8083/connectors",
		"application/json",
		bytes.NewReader([]byte(connectorConfig)))
	if err != nil {
		return err
	}

	if resp.Body == nil {
		return errors.New("Kafka Connect Rest API returned empty body")
	}
	defer resp.Body.Close()

	// not in [200, 300)
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		str, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		log.Warn(
			"Kafka Connect Rest API returned",
			zap.Int("status", resp.StatusCode),
			zap.ByteString("body", str))
		// ignore duplicated create connector
		if resp.StatusCode == http.StatusConflict {
			return nil
		}
		return errors.Errorf("Kafka Connect Rest API returned status code %d", resp.StatusCode)
	}
	return nil
}
