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

package servermaster

import (
	"bytes"
	"encoding/json"
	"net/url"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/pkg/etcdutil"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/pingcap/tiflow/pkg/security"
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

// Config is the configuration for dm-master.
type Config struct {
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

	Security *security.Credential `toml:"security" json:"security"`
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

// Adjust adjusts the master configuration
func (c *Config) Adjust() (err error) {
	c.Etcd.Adjust(defaultPeerUrls, defaultInitialClusterState)

	if c.AdvertiseAddr == "" {
		c.AdvertiseAddr = c.MasterAddr
	}

	c.KeepAliveInterval, err = time.ParseDuration(c.KeepAliveIntervalStr)
	if err != nil {
		return err
	}

	c.KeepAliveTTL, err = time.ParseDuration(c.KeepAliveTTLStr)
	if err != nil {
		return err
	}

	c.RPCTimeout, err = time.ParseDuration(c.RPCTimeoutStr)
	if err != nil {
		return err
	}
	return nil
}

// configFromFile loads config from file and merges items into Config.
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

// GetDefaultMasterConfig returns a default master config
func GetDefaultMasterConfig() *Config {
	return &Config{
		LogConf: logutil.Config{
			Level: "info",
			File:  "",
		},
		MasterAddr:    "",
		AdvertiseAddr: "",
		Etcd: &etcdutil.ConfigParams{
			PeerUrls:            defaultPeerUrls,
			InitialClusterState: defaultInitialClusterState,
		},
		FrameMetaConf:        NewFrameMetaConfig(),
		UserMetaConf:         NewDefaultUserMetaConfig(),
		KeepAliveTTLStr:      defaultKeepAliveTTL,
		KeepAliveIntervalStr: defaultKeepAliveInterval,
		RPCTimeoutStr:        defaultRPCTimeout,
	}
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
