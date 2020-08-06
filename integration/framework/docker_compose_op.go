package framework

import (
	"os/exec"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/retry"
	"go.uber.org/zap"
)

type DockerComposeOperator struct {
	fileName   string
	controller string
	healthChecker func () error
}

func (d *DockerComposeOperator) Setup() {
	cmd := exec.Command("docker-compose", "-f", d.fileName, "up", "--detach")
	runCmdHandleError(cmd)

	if d.healthChecker != nil {
		err := retry.Run(time.Second, 120, d.healthChecker)
		if err != nil {
			log.Fatal("Docker service health check failed after max retries", zap.Error(err))
		}
	}
}

func runCmdHandleError(cmd *exec.Cmd) []byte {
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
	return bytes
}

func (d *DockerComposeOperator) TearDown() {
	cmd := exec.Command("docker-compose", "-f", d.fileName, "down", "-v")
	runCmdHandleError(cmd)
}

func (d *DockerComposeOperator) ExecInController(shellCmd string) ([]byte, error) {
	cmd := exec.Command("docker", "exec", "-it", d.controller, "sh", "-c", shellCmd)
	return cmd.Output()
}