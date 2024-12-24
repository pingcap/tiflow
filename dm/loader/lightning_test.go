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
	"github.com/pingcap/tiflow/dm/config/dbconfig"
	"github.com/pingcap/tiflow/dm/config/security"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	certificate "github.com/pingcap/tiflow/pkg/security"
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
	ca2, err := certificate.NewCA()
	require.NoError(t, err)
	cert2, key2, err := ca2.GenerateCerts("dm")
	require.NoError(t, err)
	caPath2, err := certificate.WriteFile("dm-test-client-cert2", ca2.CAPEM)
	require.NoError(t, err)
	certPath2, err := certificate.WriteFile("dm-test-client-cert2", cert2)
	require.NoError(t, err)
	keyPath2, err := certificate.WriteFile("dm-test-client-key2", key2)
	require.NoError(t, err)
	cases := []struct {
		dbSecurity *security.Security
		pdSecurity *security.Security
		checkPath  bool
		err        error
	}{
		// init security with certificates file path
		{
			dbSecurity: &security.Security{SSLCA: caPath, SSLCert: certPath, SSLKey: keyPath},
			pdSecurity: &security.Security{SSLCA: caPath2, SSLCert: certPath2, SSLKey: keyPath2},
			checkPath:  true, err: nil,
		},
		{
			dbSecurity: &security.Security{SSLCA: caPath, SSLCert: certPath, SSLKey: keyPath},
			pdSecurity: &security.Security{SSLCA: caPath, SSLCert: certPath, SSLKey: keyPath},
			checkPath:  true, err: nil,
		},
		{
			dbSecurity: &security.Security{SSLCA: caPath, SSLCert: certPath, SSLKey: keyPath},
			pdSecurity: nil,
			checkPath:  true, err: nil,
		},
		{
			dbSecurity: nil,
			pdSecurity: &security.Security{SSLCA: caPath2, SSLCert: certPath2, SSLKey: keyPath2},
			checkPath:  true, err: nil,
		},
		{
			dbSecurity: &security.Security{SSLCA: "invalid/path"},
			pdSecurity: &security.Security{SSLCA: caPath2, SSLCert: certPath2, SSLKey: keyPath2},
			checkPath:  true, err: errors.New("could not read ca certificate: open invalid/path: no such file or directory"),
		},
		// init security with certificates content
		{
			dbSecurity: &security.Security{SSLCABytes: ca.CAPEM, SSLCertBytes: cert, SSLKeyBytes: key},
			pdSecurity: &security.Security{SSLCABytes: ca2.CAPEM, SSLCertBytes: cert2, SSLKeyBytes: key2},
			checkPath:  false, err: nil,
		},
		{
			dbSecurity: &security.Security{SSLCABytes: ca.CAPEM, SSLCertBytes: cert, SSLKeyBytes: key},
			pdSecurity: &security.Security{SSLCABytes: ca2.CAPEM, SSLCertBytes: cert2, SSLKeyBytes: key2, SSLCA: caPath2},
			checkPath:  true, err: nil,
		},
		{
			dbSecurity: &security.Security{SSLCABytes: ca.CAPEM, SSLCertBytes: cert, SSLKeyBytes: key},
			pdSecurity: &security.Security{SSLCABytes: ca.CAPEM, SSLCertBytes: cert, SSLKeyBytes: key},
			checkPath:  false, err: nil,
		},
		{
			dbSecurity: &security.Security{SSLCABytes: ca.CAPEM, SSLCertBytes: cert, SSLKeyBytes: key},
			pdSecurity: nil,
			checkPath:  false, err: nil,
		},
		{
			dbSecurity: nil,
			pdSecurity: &security.Security{SSLCABytes: ca2.CAPEM, SSLCertBytes: cert2, SSLKeyBytes: key2},
			checkPath:  false, err: nil,
		},
		{
			dbSecurity: &security.Security{SSLCABytes: []byte("fake ca"), SSLCertBytes: []byte("fake cert"), SSLKeyBytes: []byte("fake key")},
			pdSecurity: &security.Security{SSLCABytes: ca2.CAPEM, SSLCertBytes: cert2, SSLKeyBytes: key2},
			err:        errors.New("could not load client key pair: tls: failed to find any PEM data in certificate input"),
		},
	}
	for _, c := range cases {
		var (
			globalCfg lcfg.GlobalConfig
			dbCfg     dbconfig.DBConfig
			loaderCfg config.LoaderConfig
		)
		if c.dbSecurity != nil {
			globalCfg.Security = lcfg.Security{
				CAPath: c.dbSecurity.SSLCA, CertPath: c.dbSecurity.SSLCert, KeyPath: c.dbSecurity.SSLKey,
				CABytes: c.dbSecurity.SSLCABytes, CertBytes: c.dbSecurity.SSLCertBytes, KeyBytes: c.dbSecurity.SSLKeyBytes,
			}
			dbCfg.Security = &security.Security{
				SSLCA: c.dbSecurity.SSLCA, SSLCert: c.dbSecurity.SSLCert, SSLKey: c.dbSecurity.SSLKey,
				SSLCABytes: c.dbSecurity.SSLCABytes, SSLCertBytes: c.dbSecurity.SSLCertBytes, SSLKeyBytes: c.dbSecurity.SSLKeyBytes,
			}
		}
		if c.pdSecurity != nil {
			loaderCfg.Security = &security.Security{
				SSLCA: c.pdSecurity.SSLCA, SSLCert: c.pdSecurity.SSLCert, SSLKey: c.pdSecurity.SSLKey,
				SSLCABytes: c.pdSecurity.SSLCABytes, SSLCertBytes: c.pdSecurity.SSLCertBytes, SSLKeyBytes: c.pdSecurity.SSLKeyBytes,
			}
		}
		conf, err = GetLightningConfig(&globalCfg, &config.SubTaskConfig{To: dbCfg, LoaderConfig: loaderCfg})
		if c.err == nil {
			if c.pdSecurity != nil {
				if c.checkPath {
					require.Equal(t, loaderCfg.Security.SSLCA, conf.Security.CAPath)
					require.Equal(t, loaderCfg.Security.SSLCert, conf.Security.CertPath)
					require.Equal(t, loaderCfg.Security.SSLKey, conf.Security.KeyPath)
				}
				require.Equal(t, loaderCfg.Security.SSLCABytes, conf.Security.CABytes)
				require.Equal(t, loaderCfg.Security.SSLCertBytes, conf.Security.CertBytes)
				require.Equal(t, loaderCfg.Security.SSLKeyBytes, conf.Security.KeyBytes)
			}
			if c.dbSecurity != nil {
				if c.checkPath {
					require.Equal(t, dbCfg.Security.SSLCA, conf.TiDB.Security.CAPath)
					require.Equal(t, dbCfg.Security.SSLCert, conf.TiDB.Security.CertPath)
					require.Equal(t, dbCfg.Security.SSLKey, conf.TiDB.Security.KeyPath)
				}
				require.Equal(t, dbCfg.Security.SSLCABytes, conf.TiDB.Security.CABytes)
				require.Equal(t, dbCfg.Security.SSLCertBytes, conf.TiDB.Security.CertBytes)
				require.Equal(t, dbCfg.Security.SSLKeyBytes, conf.TiDB.Security.KeyBytes)
				if c.pdSecurity == nil {
					if c.checkPath {
						require.Equal(t, dbCfg.Security.SSLCA, conf.Security.CAPath)
						require.Equal(t, dbCfg.Security.SSLCert, conf.Security.CertPath)
						require.Equal(t, dbCfg.Security.SSLKey, conf.Security.KeyPath)
					}
					require.Equal(t, dbCfg.Security.SSLCABytes, conf.Security.CABytes)
					require.Equal(t, dbCfg.Security.SSLCertBytes, conf.Security.CertBytes)
					require.Equal(t, dbCfg.Security.SSLKeyBytes, conf.Security.KeyBytes)
				}
			}
		} else {
			require.Equal(t, c.err.Error(), err.Error())
		}
	}
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
