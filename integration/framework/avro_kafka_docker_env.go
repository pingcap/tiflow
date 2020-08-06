package framework

import (
	"encoding/json"
	"errors"
	"github.com/integralist/go-findroot/find"
	"go.uber.org/zap"
	"io/ioutil"
	"log"
	"net/http"
)

type AvroKafkaDockerEnv struct {
	DockerComposeOperator
}

func NewAvroKafkaDockerEnv() *AvroKafkaDockerEnv {
	healthChecker := func() error {
		resp, err := http.Get("http://127.0.0.1:18083")
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

	st, err := find.Repo()
	if err != nil {
		log.Fatal("Could not find git repo root", zap.Error(err))
	}

	return &AvroKafkaDockerEnv{DockerComposeOperator{
		fileName:      st.Path + "/docker-compose-avro.yml",
		controller:    "ticdc_controller_1",
		healthChecker: healthChecker,
	}}
}

func (e *AvroKafkaDockerEnv) Reset() {
	e.TearDown()
	e.Setup()
}

func (e *AvroKafkaDockerEnv) RunTest(test interface{}) {
	// TODO
}

func (e *AvroKafkaDockerEnv) SetListener(states interface{}, listener MqListener) {
	// TODO
}
