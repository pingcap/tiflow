// Copyright 2019 PingCAP, Inc.
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

package master

import (
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	capturer "github.com/kami-zh/go-capturer"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.etcd.io/etcd/server/v3/embed"
)

type testConfigSuite struct {
	suite.Suite
}

func (t *testConfigSuite) SetupSuite() {
	// initialized the logger to make genEmbedEtcdConfig working.
	t.Require().NoError(log.InitLogger(&log.Config{}))
}

func (t *testConfigSuite) TestPrintSampleConfig() {
	// test print sample config
	out := capturer.CaptureStdout(func() {
		cfg := NewConfig()
		err := cfg.Parse([]string{"-print-sample-config"})
		t.Require().Error(err)
		t.Require().Regexp(flag.ErrHelp.Error(), err.Error())
	})
	t.Require().Equal(strings.TrimSpace(SampleConfig), strings.TrimSpace(out))
}

func (t *testConfigSuite) TestConfig() {
	var (
		err        error
		cfg        = &Config{}
		masterAddr = ":8261"
		cases      = []struct {
			args     []string
			hasError bool
			errorReg string
		}{
			{
				[]string{"-V"},
				true,
				flag.ErrHelp.Error(),
			},
			{
				[]string{"-print-sample-config"},
				true,
				flag.ErrHelp.Error(),
			},
			{
				[]string{"invalid"},
				true,
				".*'invalid' is an invalid flag.*",
			},
		}
	)

	err = cfg.FromContent(SampleConfig)
	t.Require().NoError(err)
	err = cfg.Reload()
	t.Require().NoError(err)
	t.Require().Equal(masterAddr, cfg.MasterAddr)

	for _, tc := range cases {
		cfg = NewConfig()
		err = cfg.Parse(tc.args)
		if tc.hasError {
			t.Require().Error(err)
			t.Require().Regexp(tc.errorReg, err.Error())
		}
	}
}

func (t *testConfigSuite) TestInvalidConfig() {
	var (
		err error
		cfg = NewConfig()
	)

	filepath := path.Join(t.T().TempDir(), "test_invalid_config.toml")
	// field still remain undecoded in config will cause verify failed
	configContent := []byte(`
master-addr = ":8261"
advertise-addr = "127.0.0.1:8261"
aaa = "xxx"`)
	err = os.WriteFile(filepath, configContent, 0o644)
	t.Require().NoError(err)
	err = cfg.configFromFile(filepath)
	t.Require().Error(err)
	t.Require().Regexp("*master config contained unknown configuration options: aaa.*", err.Error())

	// invalid `master-addr`
	filepath2 := path.Join(t.T().TempDir(), "test_invalid_config.toml")
	configContent2 := []byte(`master-addr = ""`)
	err = os.WriteFile(filepath2, configContent2, 0o644)
	t.Require().NoError(err)
	err = cfg.configFromFile(filepath2)
	t.Require().NoError(err)
	t.Require().True(terror.ErrMasterHostPortNotValid.Equal(cfg.adjust()))
}

func (t *testConfigSuite) TestGenEmbedEtcdConfig() {
	hostname, err := os.Hostname()
	t.Require().NoError(err)

	cfg1 := NewConfig()
	cfg1.MasterAddr = ":8261"
	cfg1.AdvertiseAddr = "127.0.0.1:8261"
	cfg1.InitialClusterState = embed.ClusterStateFlagExisting
	t.Require().NoError(cfg1.adjust())
	etcdCfg, err := cfg1.genEmbedEtcdConfig(embed.NewConfig())
	t.Require().NoError(err)
	t.Require().Equal(fmt.Sprintf("dm-master-%s", hostname), etcdCfg.Name)
	t.Require().Equal(fmt.Sprintf("default.%s", etcdCfg.Name), etcdCfg.Dir)
	t.Require().Equal([]url.URL{{Scheme: "http", Host: "0.0.0.0:8261"}}, etcdCfg.ListenClientUrls)
	t.Require().Equal([]url.URL{{Scheme: "http", Host: "127.0.0.1:8261"}}, etcdCfg.AdvertiseClientUrls)
	t.Require().Equal([]url.URL{{Scheme: "http", Host: "127.0.0.1:8291"}}, etcdCfg.ListenPeerUrls)
	t.Require().Equal([]url.URL{{Scheme: "http", Host: "127.0.0.1:8291"}}, etcdCfg.AdvertisePeerUrls)
	t.Require().Equal(fmt.Sprintf("dm-master-%s=http://127.0.0.1:8291", hostname), etcdCfg.InitialCluster)
	t.Require().Equal(embed.ClusterStateFlagExisting, etcdCfg.ClusterState)
	t.Require().Equal("periodic", etcdCfg.AutoCompactionMode)
	t.Require().Equal("1h", etcdCfg.AutoCompactionRetention)
	t.Require().Equal(int64(2*1024*1024*1024), etcdCfg.QuotaBackendBytes)

	cfg2 := *cfg1
	cfg2.MasterAddr = "127.0.0.1\n:8261"
	cfg2.AdvertiseAddr = "127.0.0.1:8261"
	_, err = cfg2.genEmbedEtcdConfig(embed.NewConfig())
	t.Require().True(terror.ErrMasterGenEmbedEtcdConfigFail.Equal(err))
	t.Require().Error(err)
	t.Require().Regexp("(?m).*invalid master-addr.*", err.Error())
	cfg2.MasterAddr = "172.100.8.8:8261"
	cfg2.AdvertiseAddr = "172.100.8.8:8261"
	etcdCfg, err = cfg2.genEmbedEtcdConfig(embed.NewConfig())
	t.Require().NoError(err)
	t.Require().Equal([]url.URL{{Scheme: "http", Host: "172.100.8.8:8261"}}, etcdCfg.ListenClientUrls)
	t.Require().Equal([]url.URL{{Scheme: "http", Host: "172.100.8.8:8261"}}, etcdCfg.AdvertiseClientUrls)

	cfg3 := *cfg1
	cfg3.PeerUrls = "127.0.0.1:\n8291"
	_, err = cfg3.genEmbedEtcdConfig(embed.NewConfig())
	t.Require().True(terror.ErrMasterGenEmbedEtcdConfigFail.Equal(err))
	t.Require().Error(err)
	t.Require().Regexp("(?m).*invalid peer-urls.*", err.Error())
	cfg3.PeerUrls = "http://172.100.8.8:8291"
	etcdCfg, err = cfg3.genEmbedEtcdConfig(embed.NewConfig())
	t.Require().NoError(err)
	t.Require().Equal([]url.URL{{Scheme: "http", Host: "172.100.8.8:8291"}}, etcdCfg.ListenPeerUrls)

	cfg4 := *cfg1
	cfg4.AdvertisePeerUrls = "127.0.0.1:\n8291"
	_, err = cfg4.genEmbedEtcdConfig(embed.NewConfig())
	t.Require().True(terror.ErrMasterGenEmbedEtcdConfigFail.Equal(err))
	t.Require().Error(err)
	t.Require().Regexp("(?m).*invalid advertise-peer-urls.*", err.Error())
	cfg4.AdvertisePeerUrls = "http://172.100.8.8:8291"
	etcdCfg, err = cfg4.genEmbedEtcdConfig(embed.NewConfig())
	t.Require().NoError(err)
	t.Require().Equal([]url.URL{{Scheme: "http", Host: "172.100.8.8:8291"}}, etcdCfg.AdvertisePeerUrls)
}

func (t *testConfigSuite) TestParseURLs() {
	cases := []struct {
		str    string
		urls   []url.URL
		hasErr bool
	}{
		{}, // empty str
		{
			str:  "http://127.0.0.1:8291",
			urls: []url.URL{{Scheme: "http", Host: "127.0.0.1:8291"}},
		},
		{
			str:  "https://127.0.0.1:8291",
			urls: []url.URL{{Scheme: "https", Host: "127.0.0.1:8291"}},
		},
		{
			str: "http://127.0.0.1:8291,http://127.0.0.1:18291",
			urls: []url.URL{
				{Scheme: "http", Host: "127.0.0.1:8291"},
				{Scheme: "http", Host: "127.0.0.1:18291"},
			},
		},
		{
			str: "https://127.0.0.1:8291,https://127.0.0.1:18291",
			urls: []url.URL{
				{Scheme: "https", Host: "127.0.0.1:8291"},
				{Scheme: "https", Host: "127.0.0.1:18291"},
			},
		},
		{
			str:  "127.0.0.1:8291", // no scheme
			urls: []url.URL{{Scheme: "http", Host: "127.0.0.1:8291"}},
		},
		{
			str:  "http://:8291", // no IP
			urls: []url.URL{{Scheme: "http", Host: "0.0.0.0:8291"}},
		},
		{
			str:  ":8291", // no scheme, no IP
			urls: []url.URL{{Scheme: "http", Host: "0.0.0.0:8291"}},
		},
		{
			str:  "http://", // no IP, no port
			urls: []url.URL{{Scheme: "http", Host: ""}},
		},
		{
			str:    "http://\n127.0.0.1:8291", // invalid char in URL
			hasErr: true,
		},
		{
			str: ":8291,http://127.0.0.1:18291",
			urls: []url.URL{
				{Scheme: "http", Host: "0.0.0.0:8291"},
				{Scheme: "http", Host: "127.0.0.1:18291"},
			},
		},
		{
			str: "LancedeMacBook-Pro.local:8661",
			urls: []url.URL{
				{Scheme: "http", Host: "LancedeMacBook-Pro.local:8661"},
			},
		},
	}

	for _, cs := range cases {
		t.T().Logf("raw string %s", cs.str)
		urls, err := parseURLs(cs.str)
		if cs.hasErr {
			t.Require().True(terror.ErrMasterParseURLFail.Equal(err))
		} else {
			t.Require().NoError(err)
			t.Require().Equal(cs.urls, urls)
		}
	}
}

func (t *testConfigSuite) TestAdjustAddr() {
	cfg := NewConfig()
	t.Require().NoError(cfg.FromContent(SampleConfig))
	t.Require().NoError(cfg.adjust())

	// invalid `advertise-addr`
	cfg.AdvertiseAddr = "127.0.0.1"
	t.Require().True(terror.ErrMasterAdvertiseAddrNotValid.Equal(cfg.adjust()))
	cfg.AdvertiseAddr = "0.0.0.0:8261"
	t.Require().True(terror.ErrMasterAdvertiseAddrNotValid.Equal(cfg.adjust()))

	// clear `advertise-addr`, still invalid because no `host` in `master-addr`.
	cfg.AdvertiseAddr = ""
	t.Require().True(terror.ErrMasterHostPortNotValid.Equal(cfg.adjust()))

	cfg.MasterAddr = "127.0.0.1:8261"
	t.Require().NoError(cfg.adjust())
	t.Require().Equal(cfg.MasterAddr, cfg.AdvertiseAddr)
}

func (t *testConfigSuite) TestAdjustOpenAPI() {
	cfg := NewConfig()
	t.Require().NoError(cfg.FromContent(SampleConfig))
	t.Require().NoError(cfg.adjust())

	// test default value
	t.Require().Equal(false, cfg.OpenAPI)
	t.Require().Equal(false, cfg.ExperimentalFeatures.OpenAPI)

	// adjust openapi from experimental-features
	cfg.ExperimentalFeatures.OpenAPI = true
	t.Require().NoError(cfg.adjust())
	t.Require().Equal(true, cfg.OpenAPI)
	t.Require().Equal(false, cfg.ExperimentalFeatures.OpenAPI)

	// test from flags
	t.Require().NoError(cfg.Parse([]string{"--openapi=false", "--master-addr=127.0.0.1:8261"}))
	t.Require().NoError(cfg.adjust())
	t.Require().Equal(false, cfg.OpenAPI)
	t.Require().NoError(cfg.Parse([]string{"--openapi=true", "--master-addr=127.0.0.1:8261"}))
	t.Require().NoError(cfg.adjust())
	t.Require().Equal(true, cfg.OpenAPI)
}

func TestAdjustSecretKeyPath(t *testing.T) {
	cfg := &Config{}
	require.NoError(t, cfg.adjustSecretKeyPath())

	// non exist file
	dir := t.TempDir()
	cfg.SecretKeyPath = filepath.Join(dir, "non-exist")
	err := cfg.adjustSecretKeyPath()
	require.True(t, terror.ErrConfigSecretKeyPath.Equal(err))
	require.ErrorContains(t, err, "no such file")
	require.Nil(t, cfg.SecretKey)

	// not hex string
	require.NoError(t, os.WriteFile(filepath.Join(dir, "secret"), []byte("secret"), 0o644))
	cfg.SecretKeyPath = filepath.Join(dir, "secret")
	err = cfg.adjustSecretKeyPath()
	require.True(t, terror.ErrConfigSecretKeyPath.Equal(err))
	require.ErrorContains(t, err, "encoding/hex: invalid byte")
	require.Nil(t, cfg.SecretKey)

	// not enough length
	require.NoError(t, os.WriteFile(filepath.Join(dir, "secret-2"), []byte("2334"), 0o644))
	cfg.SecretKeyPath = filepath.Join(dir, "secret-2")
	err = cfg.adjustSecretKeyPath()
	require.True(t, terror.ErrConfigSecretKeyPath.Equal(err))
	require.ErrorContains(t, err, "hex AES-256 key of length 64")
	require.Nil(t, cfg.SecretKey)

	// works
	key := make([]byte, 32)
	_, err = rand.Read(key)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "secret-3"), []byte(" \t"+hex.EncodeToString(key)+"\r\n   \n"), 0o644))
	cfg.SecretKeyPath = filepath.Join(dir, "secret-3")
	require.NoError(t, cfg.adjustSecretKeyPath())
	require.Equal(t, key, cfg.SecretKey)
}
