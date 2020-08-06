package framework

import (
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAvroKafkaDockerEnv_Basic(t *testing.T) {
	env := NewAvroKafkaDockerEnv()
	require.NotNil(t, env)

	env.Setup()

	bytes, err := env.ExecInController("echo test")
	require.NoErrorf(t, err, "Execution returned error", func () string {
		switch err.(type) {
		case *exec.ExitError:
			return string(err.(*exec.ExitError).Stderr)
		default:
			return ""
		}
	}())
	require.Equal(t, "test\n", string(bytes))

	env.TearDown()
}
