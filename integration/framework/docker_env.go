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

package framework

import (
	"context"
	"database/sql"
	"fmt"
	"os/exec"

	"github.com/pingcap/log"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	"go.uber.org/zap"
)

const (
	// UpstreamDSN is upstream database dsn
	UpstreamDSN = "root@tcp(127.0.0.1:4000)/"
	// DownstreamDSN is downstream database dsn
	DownstreamDSN = "root@tcp(127.0.0.1:5000)/"
)

// DockerEnv represents the docker-compose service
type DockerEnv struct {
	DockerComposeOperator
}

// Reset implements Environment
func (e *DockerEnv) Reset() {
	stdout, err := e.ExecInController(`/cdc cli unsafe reset --no-confirm --pd="http://upstream-pd:2379"`)
	if err != nil {
		log.Fatal("ResetEnv: cannot reset the cdc cluster", zap.ByteString("stdout", stdout), zap.Error(err))
	}
	log.Info("ResetEnv: reset the cdc cluster", zap.ByteString("stdout", stdout))

	upstream, err := sql.Open("mysql", UpstreamDSN)
	if err != nil {
		log.Fatal("ResetEnv: cannot connect to upstream database", zap.Error(err))
	}
	defer upstream.Close()
	if err := dropAllSchemas(upstream); err != nil {
		log.Fatal("ResetEnv: error found when dropping all schema", zap.Error(err))
	}

	downstream, err := sql.Open("mysql", DownstreamDSN)
	if err != nil {
		log.Fatal("ResetEnv: cannot connect to downstream database", zap.Error(err))
	}
	defer downstream.Close()
	if err := dropAllSchemas(downstream); err != nil {
		log.Fatal("ResetEnv: error found when dropping all schema", zap.Error(err))
	}
}

// RunTest implements Environment
func (e *DockerEnv) RunTest(task Task) {
	cmdLine := "/cdc " + task.GetCDCProfile().String()
	bytes, err := e.ExecInController(cmdLine)
	if err != nil {
		log.Fatal("RunTest failed: cannot setup changefeed",
			zap.Error(err),
			zap.ByteString("stdout", bytes),
			zap.ByteString("stderr", err.(*exec.ExitError).Stderr))
	}

	upstream, err := sql.Open("mysql", UpstreamDSN)
	if err != nil {
		log.Fatal("RunTest: cannot connect to upstream database", zap.Error(err))
	}

	downstream, err := sql.Open("mysql", DownstreamDSN)
	if err != nil {
		log.Fatal("RunTest: cannot connect to downstream database", zap.Error(err))
	}

	taskCtx := &TaskContext{
		Upstream:   upstream,
		Downstream: downstream,
		Env:        e,
		WaitForReady: func() error {
			return retry.Do(context.Background(), e.HealthChecker, retry.WithBackoffBaseDelay(1000), retry.WithMaxTries(120), retry.WithIsRetryableErr(cerrors.IsRetryableError))
		},
		Ctx: context.Background(),
	}

	err = task.Prepare(taskCtx)
	if err != nil {
		e.TearDown()
		log.Fatal("RunTest: task preparation failed", zap.String("name", task.Name()), zap.Error(err))
	}

	log.Info("Start running task", zap.String("name", task.Name()))
	err = task.Run(taskCtx)
	if err != nil {
		err1 := e.DumpStdout()
		if err1 != nil {
			log.Warn("Failed to dump container logs", zap.Error(err1))
		}
		e.TearDown()
		log.Fatal("RunTest: task failed", zap.String("name", task.Name()), zap.Error(err))
	}
	log.Info("Finished running task", zap.String("name", task.Name()))
}

// SetListener implements Environment. Currently unfinished, will be used to monitor Kafka output
func (e *DockerEnv) SetListener(states interface{}, listener MqListener) {
	// TODO
}

var systemSchema = map[string]struct{}{
	"INFORMATION_SCHEMA": {},
	"METRICS_SCHEMA":     {},
	"PERFORMANCE_SCHEMA": {},
	"mysql":              {},
}

func dropAllSchemas(db *sql.DB) error {
	result, err := db.Query("show databases;")
	if err != nil {
		return err
	}
	var schemaNames []string
	var schema string
	for result.Next() {
		err := result.Scan(&schema)
		if err != nil {
			return err
		}
		schemaNames = append(schemaNames, schema)
	}
	for _, schema := range schemaNames {
		if _, ok := systemSchema[schema]; ok {
			continue
		}
		_, err := db.Exec(fmt.Sprintf("drop database %s;", schema))
		if err != nil {
			return err
		}
	}
	return nil
}
