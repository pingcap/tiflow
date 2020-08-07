package framework

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/pingcap/ticdc/pkg/retry"
	"io/ioutil"
	"net/http"
	"os/exec"
	"time"

	"github.com/integralist/go-findroot/find"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	healthCheckUri = "http://127.0.0.1:18083"
	dockerComposeFilePath = "/docker-compose-avro.yml"
	controllerContainerName = "ticdc_controller_1"
	upstreamDSN = "root@tcp(127.0.0.1:4000)/"
	downstreamDSN = "root@tcp(127.0.0.1:5000)/"
)

type AvroKafkaDockerEnv struct {
	DockerComposeOperator
}

func NewAvroKafkaDockerEnv() *AvroKafkaDockerEnv {
	healthChecker := func() error {
		resp, err := http.Get(healthCheckUri)
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
		fileName:      st.Path + dockerComposeFilePath,
		controller:    controllerContainerName,
		healthChecker: healthChecker,
	}}
}

func (e *AvroKafkaDockerEnv) Reset() {
	e.TearDown()
	e.Setup()
}

func (e *AvroKafkaDockerEnv) RunTest(task Task) {
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

	downstream, err := sql.Open("mysql", downstreamDSN)
	if err != nil {
		log.Fatal("RunTest: cannot connect to downstream database", zap.Error(err))
	}

	taskCtx := &TaskContext{
		Upstream:   upstream,
		Downstream: downstream,
		env:        e,
		waitForReady: func () error {
			return retry.Run(time.Second, 120, e.healthChecker)
		},
		Ctx: context.Background(),
	}

	err = task.Prepare(taskCtx)
	if err != nil {
		log.Fatal("RunTest: task preparation failed", zap.String("name", task.Name()), zap.Error(err))
	}

	log.Info("Start running task", zap.String("name", task.Name()))
	err = task.Run(taskCtx)
	if err != nil {
		log.Fatal("RunTest: task failed", zap.String("name", task.Name()), zap.Error(err))
	}
	log.Info("Finished running task", zap.String("name", task.Name()))
}

func (e *AvroKafkaDockerEnv) SetListener(states interface{}, listener MqListener) {
	// TODO
}