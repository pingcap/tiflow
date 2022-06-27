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

package servermaster

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetaStoreConfig(t *testing.T) {
	t.Parallel()

	testToml := `
[frame-metastore-conf]
endpoints = ["mysql-0:3306"]
auth.user = "root"
auth.passwd = "passwd"

[user-metastore-conf]
endpoints = ["metastore:12479"]
`
	fileName := mustWriteToTempFile(t, testToml)

	config := GetDefaultMasterConfig()
	err := config.ConfigFromFile(fileName)
	require.Nil(t, err)
	err = config.Adjust()
	require.Nil(t, err)

	require.Equal(t, "mysql-0:3306", config.FrameMetaConf.Endpoints[0])
	require.Equal(t, "root", config.FrameMetaConf.Auth.User)
	require.Equal(t, "passwd", config.FrameMetaConf.Auth.Passwd)
	require.Equal(t, "metastore:12479", config.UserMetaConf.Endpoints[0])
}

func mustWriteToTempFile(t *testing.T, content string) (filePath string) {
	dir := t.TempDir()
	fd, err := ioutil.TempFile(dir, "*")
	require.NoError(t, err)
	_, err = fd.WriteString(content)
	require.NoError(t, err)

	return fd.Name()
}
