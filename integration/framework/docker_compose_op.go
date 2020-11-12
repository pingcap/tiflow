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
	"database/sql"
	"os"
	"os/exec"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/retry"
	"go.uber.org/zap"
)

// DockerComposeOperator represent a docker compose
type DockerComposeOperator struct {
	FileName      string
	Controller    string
	HealthChecker func() error
	ExecEnv       []string
}

// Setup brings up a docker-compose service
func (d *DockerComposeOperator) Setup() {
	cmd := exec.Command("docker-compose", "-f", d.FileName, "up", "-d")
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, d.ExecEnv...)
	runCmdHandleError(cmd)
	err := waitTiDBStarted(UpstreamDSN)
	if err != nil {
		log.Fatal("ping upstream database but not receive a pong", zap.Error(err))
	}
	err = waitTiDBStarted(DownstreamDSN)
	if err != nil {
		log.Fatal("ping downstream database but not receive a pong", zap.Error(err))
	}

	if d.HealthChecker != nil {
		err := retry.Run(time.Second, 120, d.HealthChecker)
		if err != nil {
			log.Fatal("Docker service health check failed after max retries", zap.Error(err))
		}
	}
}

func waitTiDBStarted(dsn string) error {
	return retry.Run(time.Second, 60, func() error {
		upstream, err := sql.Open("mysql", dsn)
		if err != nil {
			return errors.Trace(err)
		}
		defer upstream.Close()
		err = upstream.Ping()
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	})
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

// DumpStdout dumps all container logs
func (d *DockerComposeOperator) DumpStdout() error {
	log.Info("Dumping container logs")
	cmd := exec.Command("docker-compose", "-f", d.FileName, "logs", "-t")
	f, err := os.Create("../docker/logs/stdout.log")
	if err != nil {
		return errors.AddStack(err)
	}
	defer f.Close()
	cmd.Stdout = f
	err = cmd.Run()
	if err != nil {
		return errors.AddStack(err)
	}

	return nil
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
