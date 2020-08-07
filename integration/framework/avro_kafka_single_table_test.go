package framework

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

type emptyAvroSingleTableTask struct {
	AvroSingleTableTask
}

func TestAvroSingleTableTest_Prepare(t *testing.T) {
	env := NewAvroKafkaDockerEnv()
	require.NotNil(t, env)

	env.Setup()
	env.RunTest(&emptyAvroSingleTableTask{AvroSingleTableTask{TableName: "test"}})

	_, err := sql.Open("mysql", upstreamDSN + "testdb")
	require.NoError(t, err)

	_, err = sql.Open("mysql", downstreamDSN + "testdb")
	require.NoError(t, err)

	err = env.healthChecker()
	require.NoError(t, err)

	env.TearDown()
}
