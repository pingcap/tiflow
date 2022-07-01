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

package servermaster

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/pkg/etcdutil"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/version"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/logutil"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

const (
	defaultSessionTTL         = 5 * time.Second
	defaultKeepAliveTTL       = "20s"
	defaultKeepAliveInterval  = "500ms"
	defaultRPCTimeout         = "3s"
	defaultMemberLoopInterval = 10 * time.Second
	defaultCampaignTimeout    = 5 * time.Second
	defaultDiscoverTicker     = 3 * time.Second
	defaultMetricInterval     = 15 * time.Second

	defaultPeerUrls            = "http://127.0.0.1:8291"
	defaultInitialClusterState = embed.ClusterStateFlagNew
)

var (
	// EnableZap enable the zap logger in embed etcd.
	EnableZap = false
	// SampleConfigFile is sample config file of dm-master
	// later we can read it from dm/master/dm-master.toml
	// and assign it to SampleConfigFile while we build dm-master.
	SampleConfigFile string
)

// NewConfig creates a config for dm-master.
func NewConfig() *Config {
	cfg := &Config{
		Etcd:          &etcdutil.ConfigParams{},
		FrameMetaConf: NewFrameMetaConfig(),
		UserMetaConf:  NewDefaultUserMetaConfig(),
	}
	cfg.flagSet = flag.NewFlagSet("dm-master", flag.ContinueOnError)
	fs := cfg.flagSet

	fs.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")
	fs.StringVar(&cfg.ConfigFile, "config", "", "path to config file")
	fs.StringVar(&cfg.MasterAddr, "master-addr", "", "master API server and status addr")
	fs.StringVar(&cfg.AdvertiseAddr, "advertise-addr", "", `advertise address for client traffic (default "${master-addr}")`)
	fs.StringVar(&cfg.LogConf.Level, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogConf.File, "log-file", "", "log file path")
	// fs.StringVar(&cfg.LogConf.LogRotate, "log-rotate", "day", "log file rotate type, hour/day")

	fs.StringVar(&cfg.Etcd.Name, "name", "", "human-readable name for this DF-master member")
	fs.StringVar(&cfg.Etcd.DataDir, "data-dir", "", "data directory for etcd using")

	fs.StringVar(&cfg.FrameMetaConf.Endpoints[0], "frame-meta-endpoints", pkgOrm.DefaultFrameMetaEndpoints, `framework metastore endpoint`)
	fs.StringVar(&cfg.FrameMetaConf.Auth.User, "frame-meta-user", pkgOrm.DefaultFrameMetaUser, `framework metastore user`)
	fs.StringVar(&cfg.FrameMetaConf.Auth.Passwd, "frame-meta-password", pkgOrm.DefaultFrameMetaPassword, `framework metastore password`)
	fs.StringVar(&cfg.UserMetaConf.Endpoints[0], "user-meta-endpoints", metaclient.DefaultUserMetaEndpoints, `user metastore endpoint`)

	fs.StringVar(&cfg.Etcd.InitialCluster, "initial-cluster", "", fmt.Sprintf("initial cluster configuration for bootstrapping, e.g. dm-master=%s", defaultPeerUrls))
	fs.StringVar(&cfg.Etcd.PeerUrls, "peer-urls", defaultPeerUrls, "URLs for peer traffic")
	fs.StringVar(&cfg.Etcd.AdvertisePeerUrls, "advertise-peer-urls", "", `advertise URLs for peer traffic (default "${peer-urls}")`)

	return cfg
}

// Config is the configuration for dm-master.
type Config struct {
	flagSet *flag.FlagSet

	LogConf logutil.Config `toml:"log" json:"log"`

	MasterAddr    string `toml:"master-addr" json:"master-addr"`
	AdvertiseAddr string `toml:"advertise-addr" json:"advertise-addr"`

	ConfigFile string `toml:"config-file" json:"config-file"`

	// etcd relative config items
	// NOTE: we use `MasterAddr` to generate `ClientUrls` and `AdvertiseClientUrls`
	// NOTE: more items will be add when adding leader election
	Etcd *etcdutil.ConfigParams `toml:"etcd" json:"etcd"`

	FrameMetaConf *metaclient.StoreConfigParams `toml:"frame-metastore-conf" json:"frame-metastore-conf"`
	UserMetaConf  *metaclient.StoreConfigParams `toml:"user-metastore-conf" json:"user-metastore-conf"`

	KeepAliveTTLStr string `toml:"keepalive-ttl" json:"keepalive-ttl"`
	// time interval string to check executor aliveness
	KeepAliveIntervalStr string `toml:"keepalive-interval" json:"keepalive-interval"`
	RPCTimeoutStr        string `toml:"rpc-timeout" json:"rpc-timeout"`

	KeepAliveTTL      time.Duration `toml:"-" json:"-"`
	KeepAliveInterval time.Duration `toml:"-" json:"-"`
	RPCTimeout        time.Duration `toml:"-" json:"-"`

	printVersion bool
}

func (c *Config) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		log.L().Error("marshal to json", zap.Reflect("master config", c), logutil.ShortError(err))
	}
	return string(cfg)
}

// Toml returns TOML format representation of config.
func (c *Config) Toml() (string, error) {
	var b bytes.Buffer

	err := toml.NewEncoder(&b).Encode(c)
	if err != nil {
		log.L().Error("fail to marshal config to toml", logutil.ShortError(err))
	}

	return b.String(), nil
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.flagSet.Parse(arguments)
	if err != nil {
		return errors.WrapError(errors.ErrMasterConfigParseFlagSet, err)
	}

	if c.printVersion {
		fmt.Print(version.GetRawInfo())
		return flag.ErrHelp
	}

	// Load config file if specified.
	if c.ConfigFile != "" {
		err = c.configFromFile(c.ConfigFile)
		if err != nil {
			return err
		}
	}

	// Parse again to replace with command line options.
	err = c.flagSet.Parse(arguments)
	if err != nil {
		return errors.WrapError(errors.ErrMasterConfigParseFlagSet, err)
	}

	if len(c.flagSet.Args()) != 0 {
		return errors.ErrMasterConfigInvalidFlag.GenWithStackByArgs(c.flagSet.Arg(0))
	}
	return c.adjust()
}

func (c *Config) adjust() (err error) {
	c.Etcd.Adjust(defaultPeerUrls, defaultInitialClusterState)

	if c.AdvertiseAddr == "" {
		c.AdvertiseAddr = c.MasterAddr
	}

	if c.KeepAliveIntervalStr == "" {
		c.KeepAliveIntervalStr = defaultKeepAliveInterval
	}
	c.KeepAliveInterval, err = time.ParseDuration(c.KeepAliveIntervalStr)
	if err != nil {
		return err
	}

	if c.KeepAliveTTLStr == "" {
		c.KeepAliveTTLStr = defaultKeepAliveTTL
	}
	c.KeepAliveTTL, err = time.ParseDuration(c.KeepAliveTTLStr)
	if err != nil {
		return err
	}

	if c.RPCTimeoutStr == "" {
		c.RPCTimeoutStr = defaultRPCTimeout
	}
	c.RPCTimeout, err = time.ParseDuration(c.RPCTimeoutStr)
	if err != nil {
		return err
	}
	return nil
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	metaData, err := toml.DecodeFile(path, c)
	if err != nil {
		return errors.WrapError(errors.ErrMasterDecodeConfigFile, err)
	}
	return checkUndecodedItems(metaData)
}

func (c *Config) configFromString(data string) error {
	metaData, err := toml.Decode(data, c)
	if err != nil {
		return errors.WrapError(errors.ErrMasterDecodeConfigFile, err)
	}
	return checkUndecodedItems(metaData)
}

func checkUndecodedItems(metaData toml.MetaData) error {
	undecoded := metaData.Undecoded()
	if len(undecoded) > 0 {
		var undecodedItems []string
		for _, item := range undecoded {
			undecodedItems = append(undecodedItems, item.String())
		}
		return errors.ErrMasterConfigUnknownItem.GenWithStackByArgs(strings.Join(undecodedItems, ","))
	}
	return nil
}

// parseURLs parse a string into multiple urls.
// if the URL in the string without protocol scheme, use `http` as the default.
// if no IP exists in the address, `0.0.0.0` is used.
func parseURLs(s string) ([]url.URL, error) {
	if s == "" {
		return nil, nil
	}

	items := strings.Split(s, ",")
	urls := make([]url.URL, 0, len(items))
	for _, item := range items {
		// tolerate valid `master-addr`, but invalid URL format. mainly caused by no protocol scheme
		if !(strings.HasPrefix(item, "http://") || strings.HasPrefix(item, "https://")) {
			prefix := "http://"
			item = prefix + item
		}
		u, err := url.Parse(item)
		if err != nil {
			return nil, errors.WrapError(errors.ErrMasterParseURLFail, err, item)
		}
		if strings.Index(u.Host, ":") == 0 {
			u.Host = "0.0.0.0" + u.Host
		}
		urls = append(urls, *u)
	}
	return urls, nil
}
