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

package loader

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	lcfg "github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

func TestSetLightningConfig(t *testing.T) {
	t.Parallel()

	stCfg := &config.SubTaskConfig{
		LoaderConfig: config.LoaderConfig{
			PoolSize: 10,
		},
	}
	l := NewLightning(stCfg, nil, "")
	cfg, err := l.getLightningConfig()
	require.NoError(t, err)
	require.Equal(t, stCfg.LoaderConfig.PoolSize, cfg.App.RegionConcurrency)
}

func TestConvertLightningError(t *testing.T) {
	t.Parallel()

	err := common.ErrChecksumMismatch.GenWithStackByArgs(1, 2, 3, 4, 5, 6)
	converted := convertLightningError(errors.Trace(err))
	require.True(t, terror.ErrLoadLightningChecksum.Equal(converted))
	require.Contains(t, converted.Error(), "checksum mismatched, KV number in source files: 4, KV number in TiDB cluster: 3")
}

func TestGetLightiningConfig(t *testing.T) {
	t.Parallel()

	conf, err := GetLightningConfig(&lcfg.GlobalConfig{},
		&config.SubTaskConfig{
			Name:       "job123",
			ExtStorage: &storage.LocalStorage{},
			LoaderConfig: config.LoaderConfig{
				RangeConcurrency: 32,
				CompressKVPairs:  "gzip",
				Analyze:          "required",
			},
		})
	require.NoError(t, err)
	require.Equal(t, lcfg.CheckpointDriverMySQL, conf.Checkpoint.Driver)
	require.Equal(t, lcfg.CheckpointRemove, conf.Checkpoint.KeepAfterSuccess)
	require.Contains(t, conf.Checkpoint.Schema, "job123")
	require.Equal(t, 32, conf.TikvImporter.RangeConcurrency)
	require.Equal(t, lcfg.CompressionGzip, conf.TikvImporter.CompressKVPairs)
	require.Equal(t, lcfg.OpLevelRequired, conf.PostRestore.Analyze)
	lightningDefaultQuota := lcfg.NewConfig().TikvImporter.DiskQuota
	// when we don't set dm loader disk quota, it should be equal to lightning's default quota
	require.Equal(t, lightningDefaultQuota, conf.TikvImporter.DiskQuota)
}
