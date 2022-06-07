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

package executor

import (
	"encoding/hex"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigDefaultLocalStoragePath(t *testing.T) {
	t.Parallel()

	testToml := `
name = "executor-1"
worker-addr = "0.0.0.0:10241"
`
	fileName := mustWriteToTempFile(t, testToml)
	cfg := NewConfig()
	err := cfg.Parse([]string{"-config", fileName})
	require.NoError(t, err)

	expectedPath := "/tmp/dfe-storage/" + hex.EncodeToString([]byte("executor-1"))
	require.Equal(t, "executor-1", cfg.Name)
	require.Equal(t, "0.0.0.0:10241", cfg.AdvertiseAddr)
	require.Equal(t, expectedPath, cfg.Storage.Local.BaseDir)
}

func TestConfigDefaultLocalStoragePathNoName(t *testing.T) {
	t.Parallel()

	testToml := `
worker-addr = "0.0.0.0:10241"
`
	fileName := mustWriteToTempFile(t, testToml)
	cfg := NewConfig()
	err := cfg.Parse([]string{"-config", fileName})
	require.NoError(t, err)

	expectedPath := "/tmp/dfe-storage/" + hex.EncodeToString([]byte("executor-0.0.0.0:10241"))
	require.Equal(t, "0.0.0.0:10241", cfg.AdvertiseAddr)
	require.Equal(t, expectedPath, cfg.Storage.Local.BaseDir)
}

func TestConfigStorage(t *testing.T) {
	t.Parallel()

	testToml := `
name = "executor-1"
worker-addr = "0.0.0.0:10241"

[storage]
local.base-dir = "/tmp/my-base-dir"
`
	fileName := mustWriteToTempFile(t, testToml)
	cfg := NewConfig()
	err := cfg.Parse([]string{"-config", fileName})
	require.NoError(t, err)

	require.Equal(t, "/tmp/my-base-dir", cfg.Storage.Local.BaseDir)
}

func mustWriteToTempFile(t *testing.T, content string) (filePath string) {
	dir := t.TempDir()
	fd, err := ioutil.TempFile(dir, "*")
	require.NoError(t, err)
	_, err = fd.WriteString(content)
	require.NoError(t, err)

	return fd.Name()
}
