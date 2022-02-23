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
	"io/ioutil"
	"net/http"

	"github.com/integralist/go-findroot/find"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/integration/framework"
	"go.uber.org/zap"
)

const (
	dockerComposeFilePath   = "/docker-compose-canal.yml"
	controllerContainerName = "ticdc_controller_1"
	// The upstream PD endpoint in docker-compose network.
	upstreamPD = "http://upstream-pd:2379"
)

// KafkaDockerEnv represents the docker-compose service defined in docker-compose-canal.yml
type KafkaDockerEnv struct {
	framework.DockerEnv
}

// NewKafkaDockerEnv creates a new KafkaDockerEnv
func NewKafkaDockerEnv(dockerComposeFile string) *KafkaDockerEnv {
	healthChecker := func() error {
		if err := checkCanalAdapterState(); err != nil {
			return err
		}
		if err := checkDbConn(framework.UpstreamDSN); err != nil {
			return err
		}
		if err := checkDbConn(framework.DownstreamDSN); err != nil {
			return err
		}
		// Also check cdc cluster.
		return framework.CdcHealthCheck(controllerContainerName, upstreamPD)
	}
	var file string
	if dockerComposeFile == "" {
		st, err := find.Repo()
		if err != nil {
			log.Fatal("Could not find git repo root", zap.Error(err))
		}
		file = st.Path + dockerComposeFilePath
	} else {
		file = dockerComposeFile
	}

	return &KafkaDockerEnv{DockerEnv: framework.DockerEnv{
		DockerComposeOperator: framework.DockerComposeOperator{
			FileName:      file,
			Controller:    controllerContainerName,
			HealthChecker: healthChecker,
		},
	}}
}

func checkDbConn(dsn string) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	if db == nil {
		return errors.New("Can not connect to " + dsn)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		return err
	}
	return nil
}

func checkCanalAdapterState() error {
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
