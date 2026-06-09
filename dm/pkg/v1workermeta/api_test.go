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

package v1workermeta

import (
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestAPI(t *testing.T) {
	// nolint:dogsled
	_, currFile, _, _ := runtime.Caller(0)
	srcMetaPath := filepath.Join(filepath.Dir(currFile), "v106_data_for_test")
	srcDBPath := filepath.Join(srcMetaPath, "kv")

	oldMetaPath := metaPath
	oldDBPath := dbPath
	defer func() {
		metaPath = oldMetaPath
		dbPath = oldDBPath
	}()

	metaPath = t.TempDir()
	dbPath = filepath.Join(metaPath, "kv")

	// copy test data to a temp directory.
	copyDir(t, dbPath, srcDBPath)

	// get subtasks meta.
	meta, err := GetSubtasksMeta()
	require.NoError(t, err)

	// verify tasks meta.
	// - task_single:
	//   - no shard task, Running stage.
	// - task_shard
	//   - shard task, Paused stage.
	require.Len(t, meta, 2)
	require.Contains(t, meta, "task_single")
	require.Contains(t, meta, "task_shard")
	require.Equal(t, pb.Stage_Running, meta["task_single"].Stage)
	require.Equal(t, pb.Stage_Paused, meta["task_shard"].Stage)

	taskSingleCfg, err := SubTaskConfigFromV1TOML(meta["task_single"].Task)
	require.NoError(t, err)
	require.False(t, taskSingleCfg.IsSharding)
	require.Equal(t, "64", taskSingleCfg.MydumperConfig.ChunkFilesize)

	taskShardCfg, err := SubTaskConfigFromV1TOML(meta["task_shard"].Task)
	require.NoError(t, err)
	require.True(t, taskShardCfg.IsSharding)
	require.Equal(t, "64", taskSingleCfg.MydumperConfig.ChunkFilesize)

	// try to get meta again, the same as before.
	meta2, err := GetSubtasksMeta()
	require.NoError(t, err)
	require.Equal(t, meta, meta2)

	// remove all metadata.
	require.NoError(t, RemoveSubtasksMeta())

	// verify removed.
	require.False(t, utils.IsDirExists(metaPath))

	// try to get meta again, nothing exists.
	meta3, err := GetSubtasksMeta()
	require.NoError(t, err)
	require.Nil(t, meta3)

	// remove empty path is invalid.
	require.True(t, terror.ErrInvalidV1WorkerMetaPath.Equal(RemoveSubtasksMeta()))

	// remove an invalid meta path.
	metaPath = t.TempDir()
	dbPath = filepath.Join(metaPath, "kv")
	require.NoError(t, os.Mkdir(dbPath, 0o644))
	require.True(t, terror.ErrInvalidV1WorkerMetaPath.Equal(RemoveSubtasksMeta()))
}

func copyDir(t *testing.T, dst, src string) {
	si, err := os.Stat(src)
	require.NoError(t, err)
	if !si.IsDir() {
		t.Fatalf("source %s is not a directory", src)
	}

	_, err = os.Stat(dst)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("fail to get stat for source %s", src)
	}
	if err == nil {
		t.Fatalf("destination %s already exists", dst)
	}

	err = os.MkdirAll(dst, si.Mode())
	require.NoError(t, err)

	entries, err := os.ReadDir(src)
	require.NoError(t, err)

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if entry.IsDir() {
			copyDir(t, dstPath, srcPath)
		} else {
			info, err := entry.Info()
			require.NoError(t, err)
			// Skip symlinks.
			if info.Mode()&os.ModeSymlink != 0 {
				continue
			}
			copyFile(t, dstPath, srcPath)
		}
	}
}

func copyFile(t *testing.T, dst, src string) {
	in, err := os.Open(src)
	require.NoError(t, err)
	defer in.Close()

	out, err := os.Create(dst)
	require.NoError(t, err)
	defer out.Close()

	_, err = io.Copy(out, in)
	require.NoError(t, err)

	si, err := os.Stat(src)
	require.NoError(t, err)
	err = os.Chmod(dst, si.Mode())
	require.NoError(t, err)
}
