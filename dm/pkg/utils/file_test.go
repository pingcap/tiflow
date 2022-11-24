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

package utils

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

func TestFile(t *testing.T) {
	// dir not exists
	require.False(t, IsFileExists("invalid-path"))
	require.False(t, IsDirExists("invalid-path"))
	size, err := GetFileSize("invalid-path")
	require.True(t, terror.ErrGetFileSize.Equal(err))
	require.Equal(t, int64(0), size)

	// dir exists
	d := t.TempDir()
	require.False(t, IsFileExists(d))
	require.True(t, IsDirExists(d))
	size, err = GetFileSize(d)
	require.True(t, terror.ErrGetFileSize.Equal(err))
	require.Equal(t, int64(0), size)

	// file not exists
	f := filepath.Join(d, "text-file")
	require.False(t, IsFileExists(f))
	require.False(t, IsDirExists(f))
	size, err = GetFileSize(f)
	require.True(t, terror.ErrGetFileSize.Equal(err))
	require.Equal(t, int64(0), size)

	// create a file
	require.NoError(t, os.WriteFile(f, []byte("some content"), 0o644))
	require.True(t, IsFileExists(f))
	require.False(t, IsDirExists(f))
	size, err = GetFileSize(f)
	require.NoError(t, err)
	require.Equal(t, int64(len("some content")), size)
}
