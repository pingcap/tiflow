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
	"github.com/pingcap/ticdc/integration/framework"
	"github.com/pingcap/ticdc/integration/framework/avro"
	"github.com/pingcap/ticdc/integration/framework/canal"
	"github.com/pingcap/ticdc/integration/tests"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var testProtocol = flag.String("protocol", "avro", "the protocol we want to test: avro or canal")
var dockerComposeFile = flag.String("docker-compose-file", "", "the path of the Docker-compose yml file")

func testAvro() {
	task := &avro.SingleTableTask{TableName: "test"}
	testCases := []framework.Task{
		tests.NewSimpleCase(task),
		tests.NewDeleteCase(task),
		tests.NewManyTypesCase(task),
		tests.NewUnsignedCase(task),
		tests.NewCompositePKeyCase(task),
		tests.NewAlterCase(task), // this case is slow, so put it last
	}

	log.SetLevel(zapcore.DebugLevel)
	env := avro.NewKafkaDockerEnv(*dockerComposeFile)
	env.Setup()

	for i := range testCases {
		env.RunTest(testCases[i])
		if i < len(testCases)-1 {
			env.Reset()
		}
	}

	env.TearDown()
}

func testCanal() {
	task := &canal.SingleTableTask{TableName: "test"}
	testCases := []framework.Task{
		tests.NewSimpleCase(task),
		tests.NewDeleteCase(task),
		tests.NewManyTypesCase(task),
		//tests.NewUnsignedCase(task), //now canal adapter can not deal with unsigned int greater than int max
		tests.NewCompositePKeyCase(task),
		//tests.NewAlterCase(task), // basic implementation can not grantee ddl dml sequence, so can not pass
	}

	log.SetLevel(zapcore.DebugLevel)
	env := canal.NewKafkaDockerEnv(*dockerComposeFile)
	env.Setup()

	for i := range testCases {
		env.RunTest(testCases[i])
		if i < len(testCases)-1 {
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
	} else {
		log.Fatal("Unknown sink protocol", zap.String("protocol", *testProtocol))
	}
}
