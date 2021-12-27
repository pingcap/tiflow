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
	"io"
	"net/http"
	"path"

	"github.com/integralist/go-findroot/find"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/tests/mq_protocol_tests/framework"
	"go.uber.org/zap"
)

const (
	kafkaURI              = "http://127.0.0.1:18083/"
	dockerComposeFilePath = framework.DockerComposeFilePathPrefix + "docker-compose-avro.yml"
)

// KafkaDockerEnv represents the docker-compose service defined in docker-compose-avro.yml
type KafkaDockerEnv struct {
	framework.DockerEnv
}

// NewKafkaDockerEnv creates a new KafkaDockerEnv
func NewKafkaDockerEnv(dockerComposeFile string) *KafkaDockerEnv {
	healthChecker := func() error {
		resp, err := http.Get(kafkaURI)
		if err != nil {
			return err
		}

		if resp.Body == nil {
			return errors.New("kafka Connect HealthCheck returns empty body")
		}
		defer func() { _ = resp.Body.Close() }()

		bytes, err := io.ReadAll(resp.Body)
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
			return errors.Errorf("kafka connect not healthy: %v", m)
		}

		// Also check cdc cluster.
		return framework.CdcHealthCheck(framework.ControllerContainerName, framework.UpstreamPD)
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

	return &KafkaDockerEnv{DockerEnv: framework.DockerEnv{
		DockerComposeOperator: framework.DockerComposeOperator{
			FileName:      file,
			Controller:    framework.ControllerContainerName,
			HealthChecker: healthChecker,
		},
	}}
}

// Setup brings up a docker-compose service
func (d *KafkaDockerEnv) Setup() {
	d.DockerEnv.Setup()
	if err := createConnector(); err != nil {
		log.Fatal("failed to create connector", zap.Error(err))
	}
}

// Reset implements Environment
func (d *KafkaDockerEnv) Reset() {
	d.DockerEnv.Reset()
	if err := d.resetSchemaRegistry(); err != nil {
		log.Fatal("failed to reset schema registry", zap.Error(err))
	}
	if err := d.resetKafkaConnector(); err != nil {
		log.Fatal("failed to reset kafka connector", zap.Error(err))
	}
}

func (d *KafkaDockerEnv) resetSchemaRegistry() error {
	subjectsURL := "http://127.0.0.1:8081/subjects/"
	resp, err := http.Get(subjectsURL)
	if err != nil {
		return err
	}
	if resp.Body == nil {
		return errors.New("get schema registry subjects returns empty body")
	}
	defer resp.Body.Close()

	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	subs := []string{}
	err = json.Unmarshal(bytes, &subs)
	if err != nil {
		return err
	}
	for _, sub := range subs {
		url := subjectsURL + sub
		req, err := http.NewRequest(http.MethodDelete, url, nil)
		if err != nil {
			return err
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer res.Body.Close()
	}
	log.Info("Deleted the schema registry subjects", zap.Any("subjects", subs))
	return nil
}

func (d *KafkaDockerEnv) resetKafkaConnector() error {
	url := "http://127.0.0.1:8083/connectors/jdbc-sink-connector-debug/"
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	return createConnector()
}
