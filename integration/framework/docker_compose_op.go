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
	"os/exec"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/retry"
	"go.uber.org/zap"
)

// DockerComposeOperator represent a docker compose
type DockerComposeOperator struct {
	FileName      string
	Controller    string
	HealthChecker func() error
}

// Setup brings up a docker-compose service
func (d *DockerComposeOperator) Setup() {
	cmd := exec.Command("docker-compose", "-f", d.FileName, "up", "-d")
	runCmdHandleError(cmd)

	if d.HealthChecker != nil {
		err := retry.Run(time.Second, 120, d.HealthChecker)
		if err != nil {
			log.Fatal("Docker service health check failed after max retries", zap.Error(err))
		}
	}
}

func runCmdHandleError(cmd *exec.Cmd) []byte {
	log.Info("Start executing command", zap.String("cmd", cmd.String()))
	bytes, err := cmd.Output()
	if err, ok := err.(*exec.ExitError); ok {
		log.Info("Running command failed", zap.ByteString("stderr", err.Stderr))
	}

	if err != nil {
		log.Fatal("Running command failed",
			zap.Error(err),
			zap.String("command", cmd.String()),
			zap.ByteString("output", bytes))
	}

	log.Info("Finished executing command", zap.String("cmd", cmd.String()))
	return bytes
}

// TearDown terminates a docker-compose service and remove all volumes
func (d *DockerComposeOperator) TearDown() {
	log.Info("Start tearing down docker-compose services")
	cmd := exec.Command("docker-compose", "-f", d.FileName, "down", "-v")
	runCmdHandleError(cmd)
	log.Info("Finished tearing down docker-compose services")
}

// ExecInController provides a way to execute commands inside a container in the service
func (d *DockerComposeOperator) ExecInController(shellCmd string) ([]byte, error) {
	log.Info("Start executing in the Controller container", zap.String("shellCmd", shellCmd))
	cmd := exec.Command("docker", "exec", d.Controller, "sh", "-c", shellCmd)
	defer log.Info("Finished executing in the Controller container", zap.String("shellCmd", shellCmd))
	return cmd.Output()
}
