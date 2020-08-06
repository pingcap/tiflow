package framework

import (
	"os/exec"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/retry"
	"go.uber.org/zap"
)

type DockerComposeOperator struct {
	fileName      string
	controller    string
	healthChecker func() error
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
	log.Info("Start tearing down docker-compose services")
	cmd := exec.Command("docker-compose", "-f", d.fileName, "down", "-v")
	runCmdHandleError(cmd)
	log.Info("Finished tearing down docker-compose services")
}

func (d *DockerComposeOperator) ExecInController(shellCmd string) ([]byte, error) {
	log.Info("Start executing in the controller container", zap.String("shellCmd", shellCmd))
	cmd := exec.Command("docker", "exec", d.controller, "sh", "-c", shellCmd)
	defer log.Info("Finished executing in the controller container", zap.String("shellCmd", shellCmd))
	return cmd.Output()
}
