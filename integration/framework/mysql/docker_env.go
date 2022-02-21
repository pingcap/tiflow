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

	"github.com/integralist/go-findroot/find"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/integration/framework"
	"go.uber.org/zap"
)

const (
	dockerComposeFilePath   = "/docker-compose-mysql.yml"
	controllerContainerName = "ticdc_controller_1"
	// The upstream PD endpoint in docker-compose network.
	upstreamPD = "http://upstream-pd:2379"
)

// DockerEnv represents the docker-compose service defined in docker-compose-canal.yml
type DockerEnv struct {
	framework.DockerEnv
}

// NewDockerEnv creates a new KafkaDockerEnv
func NewDockerEnv(dockerComposeFile string) *DockerEnv {
	healthChecker := func() error {
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

	return &DockerEnv{DockerEnv: framework.DockerEnv{
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
