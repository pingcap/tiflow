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
	"github.com/pingcap/tiflow/integration/framework"
	"github.com/pingcap/tiflow/integration/framework/avro"
	"github.com/pingcap/tiflow/integration/framework/canal"
	"github.com/pingcap/tiflow/integration/framework/mysql"
	"github.com/pingcap/tiflow/integration/tests"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	testProtocol      = flag.String("protocol", "avro", "the protocol we want to test: avro or canal")
	dockerComposeFile = flag.String("docker-compose-file", "", "the path of the Docker-compose yml file")
)

func testAvro() {
	env := avro.NewKafkaDockerEnv(*dockerComposeFile)
	env.DockerComposeOperator.ExecEnv = []string{"CDC_TIME_ZONE=America/Los_Angeles"}
	task := &avro.SingleTableTask{TableName: "test"}
	testCases := []framework.Task{
		tests.NewDateTimeCase(task),
		tests.NewSimpleCase(task),
		tests.NewDeleteCase(task),
		tests.NewManyTypesCase(task),
		tests.NewUnsignedCase(task),
		tests.NewCompositePKeyCase(task),
		tests.NewAlterCase(task), // this case is slow, so put it last
	}

	runTests(testCases, env)
}

func testCanal() {
	env := canal.NewKafkaDockerEnv(*dockerComposeFile)
	env.DockerComposeOperator.ExecEnv = []string{"USE_FLAT_MESSAGE=false"}
	task := &canal.SingleTableTask{TableName: "test"}
	testCases := []framework.Task{
		tests.NewSimpleCase(task),
		tests.NewDeleteCase(task),
		tests.NewManyTypesCase(task),
		// tests.NewUnsignedCase(task), //now canal adapter can not deal with unsigned int greater than int max
		tests.NewCompositePKeyCase(task),
		// tests.NewAlterCase(task), // basic implementation can not grantee ddl dml sequence, so can not pass
	}

	runTests(testCases, env)
}

func testCanalJSON() {
	env := canal.NewKafkaDockerEnv(*dockerComposeFile)
	env.DockerComposeOperator.ExecEnv = []string{"USE_FLAT_MESSAGE=true"}
	task := &canal.SingleTableTask{TableName: "test", UseJSON: true}
	testCases := []framework.Task{
		tests.NewSimpleCase(task),
		tests.NewDeleteCase(task),
		tests.NewManyTypesCase(task),
		// tests.NewUnsignedCase(task), //now canal adapter can not deal with unsigned int greater than int max
		tests.NewCompositePKeyCase(task),
		tests.NewAlterCase(task),
	}

	runTests(testCases, env)
}

func testMySQL() {
	env := mysql.NewDockerEnv(*dockerComposeFile)
	task := &mysql.SingleTableTask{TableName: "test"}
	testCases := []framework.Task{
		tests.NewSimpleCase(task),
		tests.NewDeleteCase(task),
		tests.NewManyTypesCase(task),
		tests.NewUnsignedCase(task),
		tests.NewCompositePKeyCase(task),
		tests.NewAlterCase(task),
	}

	runTests(testCases, env)
}

func testMySQLWithCheckingOldvValue() {
	env := mysql.NewDockerEnv(*dockerComposeFile)
	env.DockerComposeOperator.ExecEnv = []string{"GO_FAILPOINTS=github.com/pingcap/tiflow/cdc/sink/SimpleMySQLSinkTester=return(ture)"}
	task := &mysql.SingleTableTask{TableName: "test", CheckOleValue: true}
	testCases := []framework.Task{
		tests.NewSimpleCase(task),
		tests.NewDeleteCase(task),
		tests.NewManyTypesCase(task),
		tests.NewUnsignedCase(task),
		tests.NewCompositePKeyCase(task),
		tests.NewAlterCase(task),
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
	} else if *testProtocol == "mysql" {
		testMySQL()
	} else if *testProtocol == "simple-mysql-checking-old-value" {
		testMySQLWithCheckingOldvValue()
	} else {
		log.Fatal("Unknown sink protocol", zap.String("protocol", *testProtocol))
	}
}
