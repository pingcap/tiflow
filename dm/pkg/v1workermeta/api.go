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
	"os"
	"path/filepath"
	"strconv"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

// `var` rather than `const` for testing.
var (
	// metaPath is the meta data path in v1.0.x.
	metaPath = "./dm_worker_meta"
	// dbPath is the levelDB path in v1.0.x.
	dbPath = "./dm_worker_meta/kv"
)

// v1SubTaskConfig represents the subtask config in v1.0.x.
type v1SubTaskConfig struct {
	config.SubTaskConfig // embed the subtask config in v2.0.x.

	// NOTE: in v1.0.x, `ChunkFilesize` is `int64`, but in v2.0.x it's `string`.
	// (ref: https://github.com/pingcap/dm/pull/713).
	// if we decode data with v1.0.x from TOML directly,
	// an error of `toml: cannot load TOML value of type int64 into a Go string` will be reported,
	// so we overwrite it with another filed which has the same struct tag `chunk-filesize` here.
	// but if set `chunk-filesize: 64` in a YAML file, both v1.0.x (int64) and v2.0.x (string) can support it.
	ChunkFilesize int64 `yaml:"chunk-filesize" toml:"chunk-filesize" json:"chunk-filesize"` // -F, --chunk-filesize
}

// GetSubtasksMeta gets all subtasks' meta (config and status) from `dm_worker_meta` in v1.0.x.
func GetSubtasksMeta() (map[string]*pb.V1SubTaskMeta, error) {
	// check if meta data exist.
	if !utils.IsDirExists(metaPath) || !utils.IsDirExists(dbPath) {
		return nil, nil
	}

	// open levelDB to get subtasks meta.
	db, err := openDB(dbPath, defaultKVConfig)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// load subtasks' meta from levelDB.
	meta, err := newMeta(db)
	if err != nil {
		return nil, err
	}

	return meta.TasksMeta(), nil
}

// RemoveSubtasksMeta removes subtasks' metadata.
// this is often called after upgraded from v1.0.x to v2.0.x,
// so no need to handle again after re-started the DM-worker process.
func RemoveSubtasksMeta() error {
	// check is a valid v1.0.x meta path.
	if !utils.IsDirExists(metaPath) || !utils.IsDirExists(dbPath) {
		return terror.ErrInvalidV1WorkerMetaPath.Generate(filepath.Abs(metaPath))
	}

	// try to open levelDB to check.
	db, err := openDB(dbPath, defaultKVConfig)
	if err != nil {
		return terror.ErrInvalidV1WorkerMetaPath.Generate(filepath.Abs(metaPath))
	}
	defer db.Close()

	return os.RemoveAll(metaPath)
}

// SubTaskConfigFromV1TOML gets SubTaskConfig from subtask's TOML data with v1.0.x.
func SubTaskConfigFromV1TOML(data []byte) (config.SubTaskConfig, error) {
	var v1Cfg v1SubTaskConfig
	_, err := toml.Decode(string(data), &v1Cfg)
	if err != nil {
		return config.SubTaskConfig{}, terror.ErrConfigTomlTransform.Delegate(err, "decode v1 subtask config from data")
	}

	cfg := v1Cfg.SubTaskConfig
	// DM v2.0 doesn't support heartbeat, overwrite it to false
	cfg.EnableHeartbeat = false
	cfg.MydumperConfig.ChunkFilesize = strconv.FormatInt(v1Cfg.ChunkFilesize, 10)
	err = cfg.Adjust(true)
	if err != nil {
		return config.SubTaskConfig{}, terror.ErrConfigTomlTransform.Delegate(err, "transform `chunk-filesize`")
	}

	return cfg, nil
}
