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
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/pingcap/ticdc/integration/framework"
	"io/ioutil"
	"net/http"
	"os/exec"
	"path"
	"time"

	"github.com/integralist/go-findroot/find"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/retry"
	"go.uber.org/zap"
)

const (
	healthCheckURI          = "http://127.0.0.1:18083"
	dockerComposeFilePath   = "/docker-compose-avro.yml"
	controllerContainerName = "ticdc_controller_1"
	upstreamDSN             = "root@tcp(127.0.0.1:4000)/"
	downstreamDSN           = "root@tcp(127.0.0.1:5000)/"
)

// KafkaDockerEnv represents the docker-compose service defined in docker-compose-avro.yml
type KafkaDockerEnv struct {
	framework.DockerComposeOperator
}

// NewKafkaDockerEnv creates a new KafkaDockerEnv
func NewKafkaDockerEnv(dockerComposeFile string) *KafkaDockerEnv {
	healthChecker := func() error {
		resp, err := http.Get(healthCheckURI)
		if err != nil {
			return err
		}

		if resp.Body == nil {
			return errors.New("kafka Connect HealthCheck returns empty body")
		}
		defer func() { _ = resp.Body.Close() }()

		bytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		m := make(map[string]interface{})
		err = json.Unmarshal(bytes, &m)
		if err != nil {
			return err
		}

		healthy, ok := m["healthy"]
		if !ok {
			return errors.New("kafka connect healthcheck did not return health info")
		}

		if !healthy.(bool) {
			return errors.New("kafka connect not healthy")
		}

		return nil
	}

	var file string
	if dockerComposeFile == "" {
		st, err := find.Repo()
		if err != nil {
			log.Fatal("Could not find git repo root", zap.Error(err))
		}
		file = path.Join(st.Path, dockerComposeFilePath)
	} else {
		file = dockerComposeFile
	}

	return &KafkaDockerEnv{framework.DockerComposeOperator{
		FileName:      file,
		Controller:    controllerContainerName,
		HealthChecker: healthChecker,
	}}
}

// Reset implements Environment
func (e *KafkaDockerEnv) Reset() {
	e.TearDown()
	e.Setup()
}

// RunTest implements Environment
func (e *KafkaDockerEnv) RunTest(task framework.Task) {
	cmdLine := "/cdc " + task.GetCDCProfile().String()
	bytes, err := e.ExecInController(cmdLine)
	if err != nil {
		log.Fatal("RunTest failed: cannot setup changefeed",
			zap.Error(err),
			zap.ByteString("stdout", bytes),
			zap.ByteString("stderr", err.(*exec.ExitError).Stderr))
	}

	upstream, err := sql.Open("mysql", upstreamDSN)
	if err != nil {
		log.Fatal("RunTest: cannot connect to upstream database", zap.Error(err))
	}

	_, err = upstream.Exec("set @@global.tidb_enable_clustered_index=0")
	if err != nil {
		log.Info("tidb_enable_clustered_index not supported.")
	} else {
		time.Sleep(2 * time.Second)
	}

	downstream, err := sql.Open("mysql", downstreamDSN)
	if err != nil {
		log.Fatal("RunTest: cannot connect to downstream database", zap.Error(err))
	}

	taskCtx := &framework.TaskContext{
		Upstream:   upstream,
		Downstream: downstream,
		Env:        e,
		WaitForReady: func() error {
			return retry.Run(time.Second, 120, e.HealthChecker)
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
		e.TearDown()
		log.Fatal("RunTest: task failed", zap.String("name", task.Name()), zap.Error(err))
	}
	log.Info("Finished running task", zap.String("name", task.Name()))
}

// SetListener implements Environment. Currently unfinished, will be used to monitor Kafka output
func (e *KafkaDockerEnv) SetListener(states interface{}, listener framework.MqListener) {
	// TODO
}
