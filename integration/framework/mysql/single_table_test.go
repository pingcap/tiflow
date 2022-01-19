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

package mysql

import (
	"database/sql"
	"testing"

	"github.com/pingcap/tiflow/integration/framework"
	"github.com/stretchr/testify/require"
)

type emptyCanalSingleTableTask struct {
	SingleTableTask
}

func TestCanalSingleTableTest_Prepare(t *testing.T) {
	env := NewDockerEnv("")
	require.NotNil(t, env)

	env.Setup()
	env.RunTest(&emptyCanalSingleTableTask{SingleTableTask{TableName: "test"}})

	_, err := sql.Open("mysql", framework.UpstreamDSN+"testdb")
	require.NoError(t, err)

	_, err = sql.Open("mysql", framework.DownstreamDSN+"testdb")
	require.NoError(t, err)

	err = env.HealthChecker()
	require.NoError(t, err)

	env.TearDown()
}
