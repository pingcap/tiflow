// Copyright 2022 PingCAP, Inc.
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

package common

import (
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/sync_diff_inspector/config"
	"github.com/pingcap/tiflow/sync_diff_inspector/utils"
	"github.com/stretchr/testify/require"
)

func TestConnect(t *testing.T) {
	plainPsw := "dQAUoDiyb1ucWZk7"

	require.NoError(t, failpoint.Enable(
		"github.com/pingcap/tiflow/sync_diff_inspector/source/common/MustMySQLPassword",
		fmt.Sprintf("return(\"%s\")", plainPsw)))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tiflow/sync_diff_inspector/source/common/MustMySQLPassword"))
	}()

	dataSource := &config.DataSource{
		Host:     "127.0.0.1",
		Port:     4000,
		User:     "root",
		Password: utils.SecretString(plainPsw),
	}
	_, err := ConnectMySQL(dataSource.ToDriverConfig(), 2)
	require.NoError(t, err)
	dataSource.Password = utils.SecretString(base64.StdEncoding.EncodeToString([]byte(plainPsw)))
	_, err = ConnectMySQL(dataSource.ToDriverConfig(), 2)
	require.NoError(t, err)
}
