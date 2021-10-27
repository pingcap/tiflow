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
	"flag"
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"

	capturer "github.com/kami-zh/go-capturer"
	"github.com/pingcap/check"
	"go.etcd.io/etcd/embed"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
)

var (
	defaultConfigFile = "./dm-master.toml"
	_                 = check.Suite(&testConfigSuite{})
)

type testConfigSuite struct{}

func (t *testConfigSuite) SetUpSuite(c *check.C) {
	// initialized the logger to make genEmbedEtcdConfig working.
	c.Assert(log.InitLogger(&log.Config{}), check.IsNil)
}

func (t *testConfigSuite) TestPrintSampleConfig(c *check.C) {
	buf, err := os.ReadFile(defaultConfigFile)
	c.Assert(err, check.IsNil)

	// test print sample config
	out := capturer.CaptureStdout(func() {
		cfg := NewConfig()
		err = cfg.Parse([]string{"-print-sample-config"})
		c.Assert(err, check.ErrorMatches, flag.ErrHelp.Error())
	})
	c.Assert(strings.TrimSpace(out), check.Equals, strings.TrimSpace(string(buf)))
}

func (t *testConfigSuite) TestConfig(c *check.C) {
	var (
		err               error
		cfg               = &Config{}
		masterAddr        = ":8261"
		advertiseAddr     = "127.0.0.1:8261"
		name              = "dm-master"
		dataDir           = "default.dm-master"
		peerURLs          = "http://127.0.0.1:8291"
		advertisePeerURLs = "http://127.0.0.1:8291"
		initialCluster    = "dm-master=http://127.0.0.1:8291"
		cases             = []struct {
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
			{
				[]string{"--config=./dm-master.toml"},
				false,
				"",
			},
		}
	)

	cfg.ConfigFile = defaultConfigFile
	err = cfg.Reload()
	c.Assert(err, check.IsNil)
	c.Assert(cfg.MasterAddr, check.Equals, masterAddr)

	for _, tc := range cases {
		cfg = NewConfig()
		err = cfg.Parse(tc.args)
		if tc.hasError {
			c.Assert(err, check.ErrorMatches, tc.errorReg)
		} else {
			c.Assert(cfg.MasterAddr, check.Equals, masterAddr)
			c.Assert(cfg.AdvertiseAddr, check.Equals, advertiseAddr)
			c.Assert(cfg.Name, check.Equals, name)
			c.Assert(cfg.DataDir, check.Equals, dataDir)
			c.Assert(cfg.PeerUrls, check.Equals, peerURLs)
			c.Assert(cfg.AdvertisePeerUrls, check.Equals, advertisePeerURLs)
			c.Assert(cfg.InitialCluster, check.Equals, initialCluster)
			c.Assert(cfg.InitialClusterState, check.Equals, embed.ClusterStateFlagNew)
			c.Assert(cfg.Join, check.Equals, "")
			c.Assert(cfg.String(), check.Matches, fmt.Sprintf("{.*master-addr\":\"%s\".*}", masterAddr))
		}
	}
}

func (t *testConfigSuite) TestInvalidConfig(c *check.C) {
	var (
		err error
		cfg = NewConfig()
	)

	filepath := path.Join(c.MkDir(), "test_invalid_config.toml")
	// field still remain undecoded in config will cause verify failed
	configContent := []byte(`
master-addr = ":8261"
advertise-addr = "127.0.0.1:8261"
aaa = "xxx"`)
	err = os.WriteFile(filepath, configContent, 0o644)
	c.Assert(err, check.IsNil)
	err = cfg.configFromFile(filepath)
	c.Assert(err, check.NotNil)
	c.Assert(err, check.ErrorMatches, "*master config contained unknown configuration options: aaa.*")

	// invalid `master-addr`
	filepath2 := path.Join(c.MkDir(), "test_invalid_config.toml")
	configContent2 := []byte(`master-addr = ""`)
	err = os.WriteFile(filepath2, configContent2, 0o644)
	c.Assert(err, check.IsNil)
	err = cfg.configFromFile(filepath2)
	c.Assert(err, check.IsNil)
	c.Assert(terror.ErrMasterHostPortNotValid.Equal(cfg.adjust()), check.IsTrue)
}

func (t *testConfigSuite) TestGenEmbedEtcdConfig(c *check.C) {
	hostname, err := os.Hostname()
	c.Assert(err, check.IsNil)

	cfg1 := NewConfig()
	cfg1.MasterAddr = ":8261"
	cfg1.AdvertiseAddr = "127.0.0.1:8261"
	cfg1.InitialClusterState = embed.ClusterStateFlagExisting
	c.Assert(cfg1.adjust(), check.IsNil)
	etcdCfg, err := cfg1.genEmbedEtcdConfig(embed.NewConfig())
	c.Assert(err, check.IsNil)
	c.Assert(etcdCfg.Name, check.Equals, fmt.Sprintf("dm-master-%s", hostname))
	c.Assert(etcdCfg.Dir, check.Equals, fmt.Sprintf("default.%s", etcdCfg.Name))
	c.Assert(etcdCfg.LCUrls, check.DeepEquals, []url.URL{{Scheme: "http", Host: "0.0.0.0:8261"}})
	c.Assert(etcdCfg.ACUrls, check.DeepEquals, []url.URL{{Scheme: "http", Host: "127.0.0.1:8261"}})
	c.Assert(etcdCfg.LPUrls, check.DeepEquals, []url.URL{{Scheme: "http", Host: "127.0.0.1:8291"}})
	c.Assert(etcdCfg.APUrls, check.DeepEquals, []url.URL{{Scheme: "http", Host: "127.0.0.1:8291"}})
	c.Assert(etcdCfg.InitialCluster, check.DeepEquals, fmt.Sprintf("dm-master-%s=http://127.0.0.1:8291", hostname))
	c.Assert(etcdCfg.ClusterState, check.Equals, embed.ClusterStateFlagExisting)
	c.Assert(etcdCfg.AutoCompactionMode, check.Equals, "periodic")
	c.Assert(etcdCfg.AutoCompactionRetention, check.Equals, "1h")
	c.Assert(etcdCfg.QuotaBackendBytes, check.Equals, int64(2*1024*1024*1024))

	cfg2 := *cfg1
	cfg2.MasterAddr = "127.0.0.1\n:8261"
	cfg2.AdvertiseAddr = "127.0.0.1:8261"
	_, err = cfg2.genEmbedEtcdConfig(embed.NewConfig())
	c.Assert(terror.ErrMasterGenEmbedEtcdConfigFail.Equal(err), check.IsTrue)
	c.Assert(err, check.ErrorMatches, "(?m).*invalid master-addr.*")
	cfg2.MasterAddr = "172.100.8.8:8261"
	cfg2.AdvertiseAddr = "172.100.8.8:8261"
	etcdCfg, err = cfg2.genEmbedEtcdConfig(embed.NewConfig())
	c.Assert(err, check.IsNil)
	c.Assert(etcdCfg.LCUrls, check.DeepEquals, []url.URL{{Scheme: "http", Host: "172.100.8.8:8261"}})
	c.Assert(etcdCfg.ACUrls, check.DeepEquals, []url.URL{{Scheme: "http", Host: "172.100.8.8:8261"}})

	cfg3 := *cfg1
	cfg3.PeerUrls = "127.0.0.1:\n8291"
	_, err = cfg3.genEmbedEtcdConfig(embed.NewConfig())
	c.Assert(terror.ErrMasterGenEmbedEtcdConfigFail.Equal(err), check.IsTrue)
	c.Assert(err, check.ErrorMatches, "(?m).*invalid peer-urls.*")
	cfg3.PeerUrls = "http://172.100.8.8:8291"
	etcdCfg, err = cfg3.genEmbedEtcdConfig(embed.NewConfig())
	c.Assert(err, check.IsNil)
	c.Assert(etcdCfg.LPUrls, check.DeepEquals, []url.URL{{Scheme: "http", Host: "172.100.8.8:8291"}})

	cfg4 := *cfg1
	cfg4.AdvertisePeerUrls = "127.0.0.1:\n8291"
	_, err = cfg4.genEmbedEtcdConfig(embed.NewConfig())
	c.Assert(terror.ErrMasterGenEmbedEtcdConfigFail.Equal(err), check.IsTrue)
	c.Assert(err, check.ErrorMatches, "(?m).*invalid advertise-peer-urls.*")
	cfg4.AdvertisePeerUrls = "http://172.100.8.8:8291"
	etcdCfg, err = cfg4.genEmbedEtcdConfig(embed.NewConfig())
	c.Assert(err, check.IsNil)
	c.Assert(etcdCfg.APUrls, check.DeepEquals, []url.URL{{Scheme: "http", Host: "172.100.8.8:8291"}})
}

func (t *testConfigSuite) TestParseURLs(c *check.C) {
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
		c.Logf("raw string %s", cs.str)
		urls, err := parseURLs(cs.str)
		if cs.hasErr {
			c.Assert(terror.ErrMasterParseURLFail.Equal(err), check.IsTrue)
		} else {
			c.Assert(err, check.IsNil)
			c.Assert(urls, check.DeepEquals, cs.urls)
		}
	}
}

func (t *testConfigSuite) TestAdjustAddr(c *check.C) {
	cfg := NewConfig()
	c.Assert(cfg.configFromFile(defaultConfigFile), check.IsNil)
	c.Assert(cfg.adjust(), check.IsNil)

	// invalid `advertise-addr`
	cfg.AdvertiseAddr = "127.0.0.1"
	c.Assert(terror.ErrMasterAdvertiseAddrNotValid.Equal(cfg.adjust()), check.IsTrue)
	cfg.AdvertiseAddr = "0.0.0.0:8261"
	c.Assert(terror.ErrMasterAdvertiseAddrNotValid.Equal(cfg.adjust()), check.IsTrue)

	// clear `advertise-addr`, still invalid because no `host` in `master-addr`.
	cfg.AdvertiseAddr = ""
	c.Assert(terror.ErrMasterHostPortNotValid.Equal(cfg.adjust()), check.IsTrue)

	cfg.MasterAddr = "127.0.0.1:8261"
	c.Assert(cfg.adjust(), check.IsNil)
	c.Assert(cfg.AdvertiseAddr, check.Equals, cfg.MasterAddr)
}
