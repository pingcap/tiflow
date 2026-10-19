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

package framework

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDockerComposeOperator_SetupTearDown(t *testing.T) {
	// This integration-style test verifies the operator can boot and tear down
	// the avro compose stack after resolving its compose file from the repo tree.
	fileName, err := ResolveRepoPath(DockerComposeFilePathPrefix + "docker-compose-avro.yml")
	assert.NoError(t, err)

	d := &DockerComposeOperator{
		FileName:   fileName,
		Controller: "controller0",
	}
	d.Setup()
	d.TearDown()
}
