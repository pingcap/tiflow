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
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigDefaultLocalStoragePath(t *testing.T) {
	t.Parallel()

	testToml := `
name = "executor-1"
addr = "0.0.0.0:10241"
join = "127.0.0.1:10240"
`
	fileName := mustWriteToTempFile(t, testToml)
	cfg := GetDefaultExecutorConfig()
	err := cfg.configFromFile(fileName)
	require.NoError(t, err)
	err = cfg.Adjust()
	require.NoError(t, err)

	require.Equal(t, "executor-1", cfg.Name)
	require.Equal(t, "0.0.0.0:10241", cfg.AdvertiseAddr)
}

func TestConfigDefaultLocalStoragePathNoName(t *testing.T) {
	t.Parallel()

	testToml := `
addr = "0.0.0.0:10241"
join = "127.0.0.1:10240"
`
	fileName := mustWriteToTempFile(t, testToml)
	cfg := GetDefaultExecutorConfig()
	err := cfg.configFromFile(fileName)
	require.NoError(t, err)
	err = cfg.Adjust()
	require.NoError(t, err)

	require.Equal(t, "0.0.0.0:10241", cfg.AdvertiseAddr)
}

func TestConfigStorage(t *testing.T) {
	t.Parallel()

	testToml := `
name = "executor-1"
addr = "0.0.0.0:10241"
join = "127.0.0.1:10240"
`
	fileName := mustWriteToTempFile(t, testToml)
	cfg := GetDefaultExecutorConfig()
	err := cfg.configFromFile(fileName)
	require.NoError(t, err)
	err = cfg.Adjust()
	require.NoError(t, err)
}

func mustWriteToTempFile(t *testing.T, content string) (filePath string) {
	dir := t.TempDir()
	fd, err := os.CreateTemp(dir, "*")
	require.NoError(t, err)
	_, err = fd.WriteString(content)
	require.NoError(t, err)

	return fd.Name()
}
