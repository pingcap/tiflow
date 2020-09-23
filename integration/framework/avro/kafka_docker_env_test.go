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
	"github.com/pingcap/ticdc/integration/framework"
	"os/exec"
	"testing"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
)

func TestAvroKafkaDockerEnv_Basic(t *testing.T) {
	env := NewKafkaDockerEnv("")
	require.NotNil(t, env)

	env.Setup()

	bytes, err := env.ExecInController("echo test")
	require.NoErrorf(t, err, "Execution returned error", func() string {
		switch err := err.(type) {
		case *exec.ExitError:
			return string(err.Stderr)
		default:
			return ""
		}
	}())
	require.Equal(t, "test\n", string(bytes))

	env.TearDown()
}

type dummyTask struct {
	test *testing.T
}

func (t *dummyTask) Prepare(taskContext *framework.TaskContext) error {
	return nil
}

func (t *dummyTask) GetCDCProfile() *framework.CDCProfile {
	return &framework.CDCProfile{
		PDUri:   "http://upstream-pd:2379",
		SinkURI: "kafka://kafka:9092/testdb_test?protocol=avro",
		Opts:    map[string]string{"registry": "http://schema-registry:8081"},
	}
}

func (t *dummyTask) Name() string {
	return "Dummy"
}

func (t *dummyTask) Run(taskContext *framework.TaskContext) error {
	err := taskContext.Upstream.Ping()
	require.NoError(t.test, err, "Pinging upstream failed")

	err = taskContext.Downstream.Ping()
	require.NoError(t.test, err, "Pinging downstream failed")

	err = taskContext.CreateDB("testdb")
	require.NoError(t.test, err)

	log.Info("Running task")
	return nil
}

func TestAvroKafkaDockerEnv_RunTest(t *testing.T) {
	env := NewKafkaDockerEnv("")
	require.NotNil(t, env)

	env.Setup()
	env.RunTest(&dummyTask{test: t})
	env.TearDown()
}
