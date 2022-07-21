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

package fsutil

import (
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsDirWritable(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	err := IsDirWritable(dir)
	require.Nil(t, err)

	err = os.Chmod(dir, 0o400)
	require.Nil(t, err)
	me, err := user.Current()
	require.Nil(t, err)
	if me.Name == "root" || runtime.GOOS == "windows" {
		// chmod is not supported under windows.
		t.Skip("test case is running as a superuser or in windows")
	}
	err = IsDirWritable(dir)
	require.Regexp(t, ".*permission denied", err)
}

func TestIsDirAndWritable(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "file.test")

	err := IsDirAndWritable(path)
	require.Regexp(t, ".*no such file or directory", err)

	err = os.WriteFile(path, nil, 0o600)
	require.Nil(t, err)
	err = IsDirAndWritable(path)
	require.Regexp(t, ".*is not a directory", err)

	err = IsDirAndWritable(dir)
	require.Nil(t, err)
}

func TestIsDirReadWritable(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	err := IsDirReadWritable(dir)
	require.Nil(t, err)

	path := filepath.Join(dir, "/foo")
	err = IsDirReadWritable(path)
	require.Regexp(t, ".*no such file or directory", err)
}

func TestGetDiskInfo(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	info, err := GetDiskInfo(dir)
	require.Nil(t, err)
	require.NotNil(t, info)

	dir = filepath.Join(dir, "/tmp/sorter")
	info, err = GetDiskInfo(dir)
	require.Nil(t, info)
	require.Regexp(t, ".*no such file or directory", err)
}
