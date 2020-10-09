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
	"errors"
	"github.com/integralist/go-findroot/find"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/integration/framework"
	"go.uber.org/zap"
)

const (
	dockerComposeFilePath   = "/docker-compose-canal.yml"
	controllerContainerName = "ticdc_controller_1"
)

// KafkaDockerEnv represents the docker-compose service defined in docker-compose-canal.yml
type KafkaDockerEnv struct {
	framework.KafkaDockerEnv
}

// NewKafkaDockerEnv creates a new KafkaDockerEnv
func NewKafkaDockerEnv(dockerComposeFile string) *KafkaDockerEnv {
	checkDbConn := func(dns string) error {
		db, err := sql.Open("mysql", dns)
		if err != nil {
			return err
		}
		if db == nil {
			return errors.New("Can not connect to " + dns)
		}
		defer db.Close()
		err = db.Ping()
		if err != nil {
			return err
		}
		return nil
	}
	healthChecker := func() error {
		if err := checkDbConn(framework.UpstreamDSN); err != nil {
			return err
		}
		return checkDbConn(framework.DownstreamDSN)
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

	return &KafkaDockerEnv{KafkaDockerEnv: framework.KafkaDockerEnv{
		DockerComposeOperator: framework.DockerComposeOperator{
			FileName:   file,
			Controller: controllerContainerName,
			HealthChecker: healthChecker,
		},
	}}
}
