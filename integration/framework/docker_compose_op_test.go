package framework

import (
	"github.com/integralist/go-findroot/find"
	"github.com/stretchr/testify/assert"
	"testing"
)


func TestDockerComposeOperator_SetupTearDown(t *testing.T) {
	st, err := find.Repo()
	assert.NoError(t, err)

	d := &DockerComposeOperator{
		fileName:   st.Path + "/docker-compose-avro.yml",
		controller: "controller0",
	}
	d.Setup()
	d.TearDown()
}