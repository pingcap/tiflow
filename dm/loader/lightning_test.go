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

	subtaskCfg := &config.SubTaskConfig{
		Name:       "job123",
		ExtStorage: &storage.LocalStorage{},
		LoaderConfig: config.LoaderConfig{
			RangeConcurrency:           32,
			CompressKVPairs:            "gzip",
			Analyze:                    "required",
			DistSQLScanConcurrency:     120,
			IndexSerialScanConcurrency: 120,
			ChecksumTableConcurrency:   120,
		},
	}
	conf, err := GetLightningConfig(MakeGlobalConfig(subtaskCfg), subtaskCfg)
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
	// will check requirements by default
	require.True(t, conf.App.CheckRequirements)
	require.Equal(t, 120, conf.TiDB.DistSQLScanConcurrency)
	require.Equal(t, 120, conf.TiDB.IndexSerialScanConcurrency)
	require.Equal(t, 120, conf.TiDB.ChecksumTableConcurrency)
	subtaskCfg.IgnoreCheckingItems = []string{config.AllChecking}
	subtaskCfg.DistSQLScanConcurrency = 0
	subtaskCfg.IndexSerialScanConcurrency = 0
	subtaskCfg.ChecksumTableConcurrency = 0
	conf, err = GetLightningConfig(MakeGlobalConfig(subtaskCfg), subtaskCfg)
	require.Greater(t, conf.TiDB.DistSQLScanConcurrency, 0)
	require.Greater(t, conf.TiDB.IndexSerialScanConcurrency, 0)
	require.Greater(t, conf.TiDB.ChecksumTableConcurrency, 0)
	require.NoError(t, err)
	// will not check requirements when ignore all checking items
	require.False(t, conf.App.CheckRequirements)
}

func TestLightningConfigCompatibility(t *testing.T) {
	t.Parallel()

	subtaskCfg := &config.SubTaskConfig{
		SourceID:   "mysql-replica-01",
		Name:       "job123",
		Mode:       "full",
		ExtStorage: &storage.LocalStorage{},
		LoaderConfig: config.LoaderConfig{
			ImportMode:         "physical",
			SortingDirPhysical: "./dumped_dir",
			DiskQuotaPhysical:  1,
			ChecksumPhysical:   "off",
		},
	}
	require.NoError(t, subtaskCfg.Adjust(false))
	cfg, err := GetLightningConfig(MakeGlobalConfig(subtaskCfg), subtaskCfg)
	require.NoError(t, err)

	// test deprecated configurations will write to new ones
	require.Equal(t, "./dumped_dir", cfg.TikvImporter.SortedKVDir)
	require.Equal(t, 1, int(cfg.TikvImporter.DiskQuota))
	require.Equal(t, lcfg.OpLevelOff, cfg.PostRestore.Checksum)
}
