package lib

import (
	"testing"
	"github.com/ToQoz/gopwt"
	"github.com/ToQoz/gopwt/assert"
)


func TestDockerComposeOperator_SetupTearDown(t *testing.T) {
	gopwt.Empower()
	d := &DockerComposeOperator{
		fileName:   "/home/zixiong/ticdc/docker-compose-avro.yml",
		controller: "controller0",
	}
	d.Setup()

	bytes, err := d.ExecInController("echo test")
	assert.OK(t, err == nil)
	assert.OK(t, string(bytes) == "test")

	d.TearDown()
}