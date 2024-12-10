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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"
)

var (
	caContent = []byte(`-----BEGIN CERTIFICATE-----
MIIBGDCBwAIJAOjYXLFw5V1HMAoGCCqGSM49BAMCMBQxEjAQBgNVBAMMCWxvY2Fs
aG9zdDAgFw0yMDAzMTcxMjAwMzNaGA8yMjkzMTIzMTEyMDAzM1owFDESMBAGA1UE
AwwJbG9jYWxob3N0MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEglCIJD8uVBfD
kuM+UQP+VA7Srbz17WPLA0Sqc+sQ2p6fT6HYKCW60EXiZ/yEC0925iyVbXEEbX4J
xCc2Heow5TAKBggqhkjOPQQDAgNHADBEAiAILL3Zt/3NFeDW9c9UAcJ9lc92E0ZL
GNDuH6i19Fex3wIgT0ZMAKAFSirGGtcLu0emceuk+zVKjJzmYbsLdpj/JuQ=
-----END CERTIFICATE-----
`)
	certContent = []byte(`-----BEGIN CERTIFICATE-----
MIIBZDCCAQqgAwIBAgIJAIT/lgXUc1JqMAoGCCqGSM49BAMCMBQxEjAQBgNVBAMM
CWxvY2FsaG9zdDAgFw0yMDAzMTcxMjAwMzNaGA8yMjkzMTIzMTEyMDAzM1owDTEL
MAkGA1UEAwwCZG0wWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAASBA6/ltA7vErXq
9laHAmqXPa+XX34BdbZCXspDIaIElVK8tvIMs6uQh4WUc3TiKpDf1IpI5J94ZJ9G
3p2hTohwo0owSDAaBgNVHREEEzARgglsb2NhbGhvc3SHBH8AAAEwCwYDVR0PBAQD
AgWgMB0GA1UdJQQWMBQGCCsGAQUFBwMCBggrBgEFBQcDATAKBggqhkjOPQQDAgNI
ADBFAiEAx6ljJ+tNa55ypWLGNqmXlB4UdMmKmE4RSKJ8mmEelfECIG2ZmCE59rv5
wImM6KnK+vM2QnEiISH3PeYyyRzQzycu
-----END CERTIFICATE-----
`)
	keyContent = []byte(`-----BEGIN EC PARAMETERS-----
BggqhkjOPQMBBw==
-----END EC PARAMETERS-----
-----BEGIN EC PRIVATE KEY-----
MHcCAQEEICF/GDtVxhTPTP501nOu4jgwGSDY01xN+61xd9MfChw+oAoGCCqGSM49
AwEHoUQDQgAEgQOv5bQO7xK16vZWhwJqlz2vl19+AXW2Ql7KQyGiBJVSvLbyDLOr
kIeFlHN04iqQ39SKSOSfeGSfRt6doU6IcA==
-----END EC PRIVATE KEY-----
`)
	caContent2 = []byte(`-----BEGIN CERTIFICATE-----
MIIBGDCBwAIJAOjYXLFw5V1HMAoGCCqGSM49BAMCMBQxEjAQBgNVBAMMCWxvY2Fs
aG9zdDAgFw0yMDAzMTcxMjAwMzNaGA8yMjkzMTIzMTEyMDAzM1owFDESMBAGA1UE
AwwJbG9jYWxob3N0MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEglCIJD8uVBfD
kuM+UQP+VA7Srbz17WPLA0Sqc+sQ2p6fT6HYKCW60EXiZ/yEC0925iyVbXEEbX4J
xCc2Heow5TAKBggqhkjOPQQDAgNHADBEAiAILL3Zt/3NFeDW9c9UAcJ9lc92E0ZL
GNDuH6i19Fex3wIgT0ZMAKAFSirGGtcLu0emceuk+zVKjJzmYbsLdpj/JuQ=
-----END CERTIFICATE-----
`)
	certContent2 = []byte(`-----BEGIN CERTIFICATE-----
MIIBcDCCARWgAwIBAgIUNC83r8QT87G4uCeW2wUMzaDbCvAwCgYIKoZIzj0EAwIw
FDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI0MTIwNjAzNDgxMloXDTM0MTIwNDAz
NDgxMlowDzENMAsGA1UEAwwEdGlkYjBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IA
BOWs95/gIDUG116NoBZhABn6uWbSIvDva3mwsHnw9PGevSb23Q9t1kl7y1dQpMpT
lSQ/31FOIgCul/RTMYre95CjSjBIMBoGA1UdEQQTMBGCCWxvY2FsaG9zdIcEfwAA
ATALBgNVHQ8EBAMCBaAwHQYDVR0lBBYwFAYIKwYBBQUHAwIGCCsGAQUFBwMBMAoG
CCqGSM49BAMCA0kAMEYCIQDDPgmo3olaw1D/7YW3463jvuSBd4w2Z3Ai/BHgZB7d
BAIhALKIhAqB1ffI5XdSdfnznqfwX6FY9c9POlJNfkghB07e
-----END CERTIFICATE-----
`)
	keyContent2 = []byte(`-----BEGIN EC PARAMETERS-----
BggqhkjOPQMBBw==
-----END EC PARAMETERS-----
-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIMdUrYsjfC9TNSMKAcGWYB9hmKKzyxuxMfRwDGkc03PzoAoGCCqGSM49
AwEHoUQDQgAE5az3n+AgNQbXXo2gFmEAGfq5ZtIi8O9rebCwefD08Z69JvbdD23W
SXvLV1CkylOVJD/fUU4iAK6X9FMxit73kA==
-----END EC PRIVATE KEY-----
`)
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

	cases := []struct {
		globalSecurityCfg *lcfg.Security
		loaderSecurityCfg *security.Security
		toSecurityCfg     *security.Security
	}{
		{
			globalSecurityCfg: &lcfg.Security{CABytes: caContent, CertBytes: certContent, KeyBytes: keyContent},
			loaderSecurityCfg: &security.Security{SSLCABytes: caContent2, SSLCertBytes: certContent2, SSLKeyBytes: keyContent2},
			toSecurityCfg:     &security.Security{SSLCABytes: caContent, SSLCertBytes: certContent, SSLKeyBytes: keyContent},
		},
		{
			globalSecurityCfg: &lcfg.Security{CABytes: caContent},
			loaderSecurityCfg: &security.Security{SSLCABytes: caContent2, SSLCertBytes: certContent2, SSLKeyBytes: keyContent2},
			toSecurityCfg:     &security.Security{SSLCABytes: caContent},
		},
		{
			globalSecurityCfg: &lcfg.Security{CABytes: caContent, CertBytes: certContent, KeyBytes: keyContent},
			toSecurityCfg:     &security.Security{SSLCABytes: caContent, SSLCertBytes: certContent, SSLKeyBytes: keyContent},
		},
		{
			globalSecurityCfg: &lcfg.Security{CABytes: caContent},
			toSecurityCfg:     &security.Security{SSLCABytes: caContent},
		},
		{
			globalSecurityCfg: &lcfg.Security{},
			toSecurityCfg:     &security.Security{},
		},
	}
	for _, c := range cases {
		conf, err = GetLightningConfig(
			&lcfg.GlobalConfig{Security: *c.globalSecurityCfg},
			&config.SubTaskConfig{
				LoaderConfig: config.LoaderConfig{Security: c.loaderSecurityCfg},
				To:           dbconfig.DBConfig{Security: c.toSecurityCfg},
			})
		require.NoError(t, err)
		require.Equal(t, c.globalSecurityCfg.CABytes, conf.TiDB.Security.CABytes)
		require.Equal(t, c.globalSecurityCfg.CertBytes, conf.TiDB.Security.CertBytes)
		require.Equal(t, c.globalSecurityCfg.KeyBytes, conf.TiDB.Security.KeyBytes)
		if c.loaderSecurityCfg == nil {
			require.Equal(t, c.globalSecurityCfg.CABytes, conf.Security.CABytes)
			require.Equal(t, c.globalSecurityCfg.CertBytes, conf.Security.CertBytes)
			require.Equal(t, c.globalSecurityCfg.KeyBytes, conf.Security.KeyBytes)
		} else {
			require.Equal(t, c.loaderSecurityCfg.SSLCABytes, conf.Security.CABytes)
			require.Equal(t, c.loaderSecurityCfg.SSLCertBytes, conf.Security.CertBytes)
			require.Equal(t, c.loaderSecurityCfg.SSLKeyBytes, conf.Security.KeyBytes)
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
