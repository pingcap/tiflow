package framework

import (
	"context"
	"database/sql"
	"os/exec"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/retry"
	"go.uber.org/zap"
)

const (
	UpstreamDSN   = "root@tcp(127.0.0.1:4000)/"
	DownstreamDSN = "root@tcp(127.0.0.1:5000)/"
)

// KafkaDockerEnv represents the docker-compose service defined in docker-compose-avro.yml
type KafkaDockerEnv struct {
	DockerComposeOperator
}

// Reset implements Environment
func (e *KafkaDockerEnv) Reset() {
	e.TearDown()
	e.Setup()
}

// RunTest implements Environment
func (e *KafkaDockerEnv) RunTest(task Task) {
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

	_, err = upstream.Exec("set @@global.tidb_enable_clustered_index=0")
	if err != nil {
		log.Info("tidb_enable_clustered_index not supported.")
	} else {
		time.Sleep(2 * time.Second)
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
func (e *KafkaDockerEnv) SetListener(states interface{}, listener MqListener) {
	// TODO
}
