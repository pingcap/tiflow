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

package main

import (
	"flag"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/tests/mq_protocol_tests/cases"
	"github.com/pingcap/tiflow/tests/mq_protocol_tests/framework"
	"github.com/pingcap/tiflow/tests/mq_protocol_tests/framework/avro"
	"github.com/pingcap/tiflow/tests/mq_protocol_tests/framework/canal"
	"github.com/pingcap/tiflow/tests/mq_protocol_tests/framework/mysql"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	testProtocol      = flag.String("protocol", "avro", "the protocol we want to test: avro or canal")
	dockerComposeFile = flag.String("docker-compose-file", "", "the path of the Docker-compose yml file")
)

func testAvro() {
	env := avro.NewKafkaDockerEnv(*dockerComposeFile)
	task := &avro.SingleTableTask{TableName: "test"}
	testCases := []framework.Task{
		cases.NewAlterCase(task), // this case is slow, so put it last
		cases.NewDateTimeCase(task),
		cases.NewSimpleCase(task),
		cases.NewDeleteCase(task),
		cases.NewManyTypesCase(task),
		cases.NewUnsignedCase(task),
		cases.NewCompositePKeyCase(task),
		cases.NewHandleKeyCase(task),
	}

	runTests(testCases, env)
}

func testCanal() {
	env := canal.NewKafkaDockerEnv(*dockerComposeFile)
	env.DockerComposeOperator.ExecEnv = []string{"USE_FLAT_MESSAGE=false"}
	task := &canal.SingleTableTask{TableName: "test"}
	testCases := []framework.Task{
		cases.NewSimpleCase(task),
		cases.NewDeleteCase(task),
		cases.NewManyTypesCase(task),
		// cases.NewUnsignedCase(task),
		cases.NewCompositePKeyCase(task),
		// tests.NewAlterCase(task), // basic implementation can not grantee ddl dml sequence, so can not pass
	}

	runTests(testCases, env)
}

func testCanalJSON() {
	env := canal.NewKafkaDockerEnv(*dockerComposeFile)
	env.DockerComposeOperator.ExecEnv = []string{"USE_FLAT_MESSAGE=true"}
	task := &canal.SingleTableTask{TableName: "test", UseJSON: true}
	testCases := []framework.Task{
		cases.NewSimpleCase(task),
		cases.NewDeleteCase(task),
		cases.NewManyTypesCase(task),
		// cases.NewUnsignedCase(task), //now canal adapter can not deal with unsigned int greater than int max
		cases.NewCompositePKeyCase(task),
		cases.NewAlterCase(task),
	}

	runTests(testCases, env)
}

func testCanalJSONWatermark() {
	env := canal.NewKafkaDockerEnv(*dockerComposeFile)
	env.DockerComposeOperator.ExecEnv = []string{"USE_FLAT_MESSAGE=true"}
	task := &canal.SingleTableTask{TableName: "test", UseJSON: true, EnableTiDBExtension: true}
	testCases := []framework.Task{
		cases.NewSimpleCase(task),
		cases.NewDeleteCase(task),
		cases.NewManyTypesCase(task),
		// tests.NewUnsignedCase(task), //now canal adapter can not deal with unsigned int greater than int max
		cases.NewCompositePKeyCase(task),
		cases.NewAlterCase(task),
	}

	runTests(testCases, env)
}

func testMySQL() {
	env := mysql.NewDockerEnv(*dockerComposeFile)
	task := &mysql.SingleTableTask{TableName: "test"}
	testCases := []framework.Task{
		cases.NewSimpleCase(task),
		cases.NewDeleteCase(task),
		cases.NewManyTypesCase(task),
		cases.NewUnsignedCase(task),
		cases.NewCompositePKeyCase(task),
		cases.NewAlterCase(task),
	}

	runTests(testCases, env)
}

func runTests(cases []framework.Task, env framework.Environment) {
	log.SetLevel(zapcore.DebugLevel)

	for i := range cases {
		env.Setup()
		env.RunTest(cases[i])
		if i < len(cases)-1 {
			env.Reset()
		}
	}

	env.TearDown()
}

func main() {
	flag.Parse()
	if *testProtocol == "avro" {
		testAvro()
	} else if *testProtocol == "canal" {
		testCanal()
	} else if *testProtocol == "canalJson" {
		testCanalJSON()
	} else if *testProtocol == "canalJson-extension" {
		testCanalJSONWatermark()
	} else if *testProtocol == "mysql" {
		testMySQL()
	} else {
		log.Fatal("Unknown sink protocol", zap.String("protocol", *testProtocol))
	}
}
