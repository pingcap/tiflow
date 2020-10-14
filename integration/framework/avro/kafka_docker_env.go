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
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"path"

	"github.com/integralist/go-findroot/find"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/integration/framework"
	"go.uber.org/zap"
)

const (
	healthCheckURI          = "http://127.0.0.1:18083"
	dockerComposeFilePath   = "/docker-compose-avro.yml"
	controllerContainerName = "ticdc_controller_1"
)

// KafkaDockerEnv represents the docker-compose service defined in docker-compose-avro.yml
type KafkaDockerEnv struct {
	framework.KafkaDockerEnv
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

		if v, ok := healthy.(bool); !ok || !v {
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

	return &KafkaDockerEnv{KafkaDockerEnv: framework.KafkaDockerEnv{
		DockerComposeOperator: framework.DockerComposeOperator{
			FileName:      file,
			Controller:    controllerContainerName,
			HealthChecker: healthChecker,
		},
	}}
}
