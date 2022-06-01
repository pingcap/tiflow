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

package resourcejob

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"path/filepath"
	"testing"

	brStorage "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFileWriter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	storage := makeStorageForTesting(t, dir)

	fw, err := newFileWriter(ctx, storage, "1.txt")
	require.NoError(t, err)

	err = fw.AppendInt64(ctx, 1)
	require.NoError(t, err)
	err = fw.AppendInt64(ctx, 2)
	require.NoError(t, err)
	err = fw.AppendInt64(ctx, 3)
	require.NoError(t, err)

	require.NoError(t, fw.Close(ctx))

	filePath := filepath.Join(dir, "1.txt")
	content, err := ioutil.ReadFile(filePath)
	require.NoError(t, err)

	expectedStr := "1\n2\n3\n"
	require.Equal(t, []byte(expectedStr), content)
}

func TestFileWriterOverwrite(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	filePath := filepath.Join(dir, "1.txt")
	err := ioutil.WriteFile(filePath, []byte("abcabc"), 0o666)
	require.NoError(t, err)

	storage := makeStorageForTesting(t, dir)

	fw, err := newFileWriter(ctx, storage, "1.txt")
	require.NoError(t, err)
	err = fw.AppendInt64(ctx, 1)
	require.NoError(t, err)
	require.NoError(t, fw.Close(ctx))

	content, err := ioutil.ReadFile(filePath)
	require.NoError(t, err)

	expectedStr := "1\n"
	require.Equal(t, []byte(expectedStr), content)
}

func TestFileReader(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()
	filePath := filepath.Join(dir, "1.txt")
	err := ioutil.WriteFile(filePath, []byte("1\n2\n3\n"), 0o666)
	require.NoError(t, err)

	storage := makeStorageForTesting(t, dir)
	fr, err := newFileReader(ctx, storage, "1.txt")
	require.NoError(t, err)

	out, err := fr.ReadInt64(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), out)

	out, err = fr.ReadInt64(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), out)

	out, err = fr.ReadInt64(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), out)

	_, err = fr.ReadInt64(ctx)
	require.Error(t, err)
	require.True(t, errors.Is(err, io.EOF))

	require.NoError(t, fr.Close())
}

func makeStorageForTesting(t *testing.T, dir string) brStorage.ExternalStorage {
	storage, err := brStorage.NewLocalStorage(dir)
	require.NoError(t, err)
	return storage
}
