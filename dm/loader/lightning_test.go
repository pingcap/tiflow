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
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/common"
	lcfg "github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tiflow/dm/config"
	certificate "github.com/pingcap/tiflow/pkg/security"

	"github.com/pingcap/tiflow/dm/config/dbconfig"
	"github.com/pingcap/tiflow/dm/config/security"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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

	conf, err = GetLightningConfig(&lcfg.GlobalConfig{},
		&config.SubTaskConfig{
			Name: "job123",
			LoaderConfig: config.LoaderConfig{
				RangeConcurrency: 32,
				CompressKVPairs:  "gzip",
				Analyze:          "required",
				Dir:              "/tmp",
			},
		})
	require.NoError(t, err)
	require.Equal(t, lcfg.CheckpointDriverFile, conf.Checkpoint.Driver)

	conf, err = GetLightningConfig(&lcfg.GlobalConfig{},
		&config.SubTaskConfig{
			Name: "job123",
			LoaderConfig: config.LoaderConfig{
				RangeConcurrency: 32,
				CompressKVPairs:  "gzip",
				Analyze:          "required",
				Dir:              "gcs://bucket/path",
			},
		})
	require.NoError(t, err)
	require.Equal(t, lcfg.CheckpointDriverMySQL, conf.Checkpoint.Driver)

	ca, err := certificate.NewCA()
	require.NoError(t, err)
	cert, key, err := ca.GenerateCerts("dm")
	require.NoError(t, err)
	caPath, err := certificate.WriteFile("dm-test-client-cert", ca.CAPEM)
	require.NoError(t, err)
	certPath, err := certificate.WriteFile("dm-test-client-cert", cert)
	require.NoError(t, err)
	keyPath, err := certificate.WriteFile("dm-test-client-key", key)
	require.NoError(t, err)
	ca, err = certificate.NewCA()
	require.NoError(t, err)
	cert, key, err = ca.GenerateCerts("dm")
	require.NoError(t, err)
	caPath2, err := certificate.WriteFile("dm-test-client-cert2", ca.CAPEM)
	require.NoError(t, err)
	certPath2, err := certificate.WriteFile("dm-test-client-cert2", cert)
	require.NoError(t, err)
	keyPath2, err := certificate.WriteFile("dm-test-client-key2", key)
	require.NoError(t, err)

	conf, err = GetLightningConfig(
		&lcfg.GlobalConfig{Security: lcfg.Security{CAPath: caPath, CertPath: certPath, KeyPath: keyPath}},
		&config.SubTaskConfig{
			LoaderConfig: config.LoaderConfig{Security: &security.Security{SSLCA: caPath, SSLCert: certPath, SSLKey: keyPath}},
			To:           dbconfig.DBConfig{Security: &security.Security{SSLCA: caPath2, SSLCert: certPath2, SSLKey: keyPath2}},
		})
	require.NoError(t, err)
	require.Equal(t, conf.Security.CAPath, caPath)
	require.Equal(t, conf.Security.CertPath, certPath)
	require.Equal(t, conf.Security.KeyPath, keyPath)
	require.Equal(t, conf.TiDB.Security.CAPath, caPath2)
	require.Equal(t, conf.TiDB.Security.CertPath, certPath2)
	require.Equal(t, conf.TiDB.Security.KeyPath, keyPath2)
	conf, err = GetLightningConfig(
		&lcfg.GlobalConfig{Security: lcfg.Security{CAPath: caPath, CertPath: certPath, KeyPath: keyPath}},
		&config.SubTaskConfig{
			LoaderConfig: config.LoaderConfig{Security: &security.Security{SSLCA: caPath, SSLCert: certPath, SSLKey: keyPath}},
			To:           dbconfig.DBConfig{},
		})
	require.NoError(t, err)
	require.Equal(t, conf.Security.CAPath, caPath)
	require.Equal(t, conf.Security.CertPath, certPath)
	require.Equal(t, conf.Security.KeyPath, keyPath)
	require.Equal(t, conf.TiDB.Security.CAPath, caPath)
	require.Equal(t, conf.TiDB.Security.CertPath, certPath)
	require.Equal(t, conf.TiDB.Security.KeyPath, keyPath)
	conf, err = GetLightningConfig(
		&lcfg.GlobalConfig{},
		&config.SubTaskConfig{
			LoaderConfig: config.LoaderConfig{},
			To:           dbconfig.DBConfig{Security: &security.Security{SSLCA: caPath2, SSLCert: certPath2, SSLKey: keyPath2}},
		})
	require.NoError(t, err)
	require.Equal(t, conf.Security.CAPath, "")
	require.Equal(t, conf.Security.CertPath, "")
	require.Equal(t, conf.Security.KeyPath, "")
	require.Equal(t, conf.TiDB.Security.CAPath, caPath2)
	require.Equal(t, conf.TiDB.Security.CertPath, certPath2)
	require.Equal(t, conf.TiDB.Security.KeyPath, keyPath2)
	// invalid security file path
	_, err = GetLightningConfig(
		&lcfg.GlobalConfig{Security: lcfg.Security{CAPath: "caPath"}},
		&config.SubTaskConfig{
			To: dbconfig.DBConfig{Security: &security.Security{SSLCA: "caPath"}},
		})
	require.EqualError(t, err, "could not read ca certificate: open caPath: no such file or directory")
}

func TestMetricProxies(t *testing.T) {
	l := &LightningLoader{cfg: &config.SubTaskConfig{}}
	l.initMetricProxies()
	require.Equal(t, defaultMetricProxies, l.metricProxies)

	registry := prometheus.NewRegistry()
	l = &LightningLoader{cfg: &config.SubTaskConfig{MetricsFactory: promauto.With(registry)}}
	l.initMetricProxies()
	require.NotEqual(t, defaultMetricProxies, l.metricProxies)
	l.metricProxies.loaderExitWithErrorCounter.WithLabelValues("test", "source", "false").Inc()
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, metricFamilies, 1)
	l.removeLabelValuesWithTaskInMetrics("test", "source")
	metricFamilies, err = registry.Gather()
	require.NoError(t, err)
	require.Len(t, metricFamilies, 0)
}
