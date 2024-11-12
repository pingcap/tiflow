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

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

type testAPI struct{}

var _ = check.Suite(&testAPI{})

func (t *testAPI) TestAPI(c *check.C) {
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

	metaPath = c.MkDir()
	dbPath = filepath.Join(metaPath, "kv")

	// copy test data to a temp directory.
	copyDir(c, dbPath, srcDBPath)

	// get subtasks meta.
	meta, err := GetSubtasksMeta()
	c.Assert(err, check.IsNil)

	// verify tasks meta.
	// - task_single:
	//   - no shard task, Running stage.
	// - task_shard
	//   - shard task, Paused stage.
	c.Assert(meta, check.HasLen, 2)
	c.Assert(meta, check.HasKey, "task_single")
	c.Assert(meta, check.HasKey, "task_shard")
	c.Assert(meta["task_single"].Stage, check.Equals, pb.Stage_Running)
	c.Assert(meta["task_shard"].Stage, check.Equals, pb.Stage_Paused)

	taskSingleCfg, err := SubTaskConfigFromV1TOML(meta["task_single"].Task)
	c.Assert(err, check.IsNil)
	c.Assert(taskSingleCfg.IsSharding, check.IsFalse)
	c.Assert(taskSingleCfg.MydumperConfig.ChunkFilesize, check.Equals, "64")

	taskShardCfg, err := SubTaskConfigFromV1TOML(meta["task_shard"].Task)
	c.Assert(err, check.IsNil)
	c.Assert(taskShardCfg.IsSharding, check.IsTrue)
	c.Assert(taskSingleCfg.MydumperConfig.ChunkFilesize, check.Equals, "64")

	// try to get meta again, the same as before.
	meta2, err := GetSubtasksMeta()
	c.Assert(err, check.IsNil)
	c.Assert(meta2, check.DeepEquals, meta)

	// remove all metadata.
	c.Assert(RemoveSubtasksMeta(), check.IsNil)

	// verify removed.
	c.Assert(utils.IsDirExists(metaPath), check.IsFalse)

	// try to get meta again, nothing exists.
	meta3, err := GetSubtasksMeta()
	c.Assert(err, check.IsNil)
	c.Assert(meta3, check.IsNil)

	// remove empty path is invalid.
	c.Assert(terror.ErrInvalidV1WorkerMetaPath.Equal(RemoveSubtasksMeta()), check.IsTrue)

	// remove an invalid meta path.
	metaPath = c.MkDir()
	dbPath = filepath.Join(metaPath, "kv")
	c.Assert(os.Mkdir(dbPath, 0o644), check.IsNil)
	c.Assert(terror.ErrInvalidV1WorkerMetaPath.Equal(RemoveSubtasksMeta()), check.IsTrue)
}

func copyDir(c *check.C, dst, src string) {
	si, err := os.Stat(src)
	c.Assert(err, check.IsNil)
	if !si.IsDir() {
		c.Fatalf("source %s is not a directory", src)
	}

	_, err = os.Stat(dst)
	if err != nil && !os.IsNotExist(err) {
		c.Fatalf("fail to get stat for source %s", src)
	}
	if err == nil {
		c.Fatalf("destination %s already exists", dst)
	}

	err = os.MkdirAll(dst, si.Mode())
	c.Assert(err, check.IsNil)

	entries, err := os.ReadDir(src)
	c.Assert(err, check.IsNil)

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if entry.IsDir() {
			copyDir(c, dstPath, srcPath)
		} else {
			info, err := entry.Info()
			c.Assert(err, check.IsNil)
			// Skip symlinks.
			if info.Mode()&os.ModeSymlink != 0 {
				continue
			}
			copyFile(c, dstPath, srcPath)
		}
	}
}

func copyFile(c *check.C, dst, src string) {
	in, err := os.Open(src)
	c.Assert(err, check.IsNil)
	defer in.Close()

	out, err := os.Create(dst)
	c.Assert(err, check.IsNil)
	defer out.Close()

	_, err = io.Copy(out, in)
	c.Assert(err, check.IsNil)

	si, err := os.Stat(src)
	c.Assert(err, check.IsNil)
	err = os.Chmod(dst, si.Mode())
	c.Assert(err, check.IsNil)
}
