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
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/stretchr/testify/require"
)

func TestMetaStoreConfig(t *testing.T) {
	t.Parallel()

	testToml := `
[framework-metastore-conf]
endpoints = ["mysql-0:3306"]
auth.user = "root"
auth.passwd = "passwd"
schema = "test0"

[business-metastore-conf]
endpoints = ["metastore:12479"]
store-type = "etcd"
`
	fileName := mustWriteToTempFile(t, testToml)

	config := GetDefaultMasterConfig()
	err := util.StrictDecodeFile(fileName, "tiflow master", config)
	require.Nil(t, err)
	err = config.AdjustAndValidate()
	require.Nil(t, err)

	require.Equal(t, "mysql-0:3306", config.FrameMetaConf.Endpoints[0])
	require.Equal(t, "root", config.FrameMetaConf.Auth.User)
	require.Equal(t, "passwd", config.FrameMetaConf.Auth.Passwd)
	require.Equal(t, "test0", config.FrameMetaConf.Schema)
	require.Equal(t, "mysql", config.FrameMetaConf.StoreType)

	require.Equal(t, "etcd", config.BusinessMetaConf.StoreType)
	require.Equal(t, "metastore:12479", config.BusinessMetaConf.Endpoints[0])
	require.Empty(t, config.BusinessMetaConf.Schema)
}

func mustWriteToTempFile(t *testing.T, content string) (filePath string) {
	dir := t.TempDir()
	fd, err := os.CreateTemp(dir, "*")
	require.NoError(t, err)
	_, err = fd.WriteString(content)
	require.NoError(t, err)

	return fd.Name()
}

func TestDefaultMetaStoreManager(t *testing.T) {
	t.Parallel()

	store := newFrameMetaConfig()
	require.Equal(t, FrameMetaID, store.StoreID)
	require.Equal(t, defaultFrameMetaEndpoints, store.Endpoints[0])

	store = NewDefaultBusinessMetaConfig()
	require.Equal(t, DefaultBusinessMetaID, store.StoreID)
	require.Equal(t, defaultBusinessMetaEndpoints, store.Endpoints[0])
}

func TestLoadConfigFromFile(t *testing.T) {
	t.Parallel()
	cfg := GetDefaultMasterConfig()
	data, err := cfg.Toml()
	require.NoError(t, err)

	dir := t.TempDir()
	filename := filepath.Join(dir, "master-ut.toml")
	err = os.WriteFile(filename, []byte(data), 0o600)
	require.NoError(t, err)

	cfg2 := &Config{}
	err = util.StrictDecodeFile(filename, "tiflow master", cfg2)
	require.NoError(t, err)
	require.Equal(t, cfg, cfg2)
}
