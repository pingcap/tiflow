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

package executor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/label"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/pingcap/tiflow/pkg/security"
)

var (
	defaultJoinAddr          = "127.0.0.1:10240"
	defaultKeepAliveTTL      = "20s"
	defaultKeepAliveInterval = "500ms"
	defaultRPCTimeout        = "3s"
	defaultMetricInterval    = 15 * time.Second
	defaultExecutorAddr      = "127.0.0.1:10340"
)

// Config is the configuration.
type Config struct {
	Name string `toml:"name" json:"name"`

	LogConf logutil.Config `toml:"log" json:"log"`

	Join          string `toml:"join" json:"join" `
	Addr          string `toml:"addr" json:"addr"`
	AdvertiseAddr string `toml:"advertise-addr" json:"advertise-addr"`

	Labels map[string]string `toml:"labels" json:"labels"`
	// EnableGCTuning enables a GC tuning mechanism that adjusts the GC frequency
	// according to the used memory with reference to the total memory. It can be
	// enabled when the executor can consume almost all the memory of the
	// container/machine.
	EnableGCTuning bool `toml:"enable-gc-tuning" json:"enable-gc-tuning"`

	// TODO: in the future executors should share a same ttl from server-master
	KeepAliveTTLStr      string `toml:"keepalive-ttl" json:"keepalive-ttl"`
	KeepAliveIntervalStr string `toml:"keepalive-interval" json:"keepalive-interval"`
	RPCTimeoutStr        string `toml:"rpc-timeout" json:"rpc-timeout"`

	KeepAliveTTL      time.Duration `toml:"-" json:"-"`
	KeepAliveInterval time.Duration `toml:"-" json:"-"`
	RPCTimeout        time.Duration `toml:"-" json:"-"`

	Security *security.Credential `toml:"security" json:"security"`
}

// String implements fmt.Stringer
func (c *Config) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		log.Error("fail to marshal config to json", logutil.ShortError(err))
	}
	return string(cfg)
}

// Toml returns TOML format representation of config.
func (c *Config) Toml() (string, error) {
	var b bytes.Buffer

	err := toml.NewEncoder(&b).Encode(c)
	if err != nil {
		log.Error("fail to marshal config to toml", logutil.ShortError(err))
	}

	return b.String(), nil
}

// configFromFile loads config from file and merges items into Config.
func (c *Config) configFromFile(path string) error {
	metaData, err := toml.DecodeFile(path, c)
	if err != nil {
		return errors.WrapError(errors.ErrExecutorDecodeConfigFile, err)
	}
	undecoded := metaData.Undecoded()
	if len(undecoded) > 0 && err == nil {
		var undecodedItems []string
		for _, item := range undecoded {
			undecodedItems = append(undecodedItems, item.String())
		}
		return errors.ErrExecutorConfigUnknownItem.GenWithStackByArgs(strings.Join(undecodedItems, ","))
	}
	return nil
}

// Adjust adjusts the executor configuration
func (c *Config) Adjust() (err error) {
	if c.AdvertiseAddr == "" {
		c.AdvertiseAddr = c.Addr
	}

	if c.Name == "" {
		c.Name = fmt.Sprintf("executor-%s", c.AdvertiseAddr)
	}

	c.KeepAliveInterval, err = time.ParseDuration(c.KeepAliveIntervalStr)
	if err != nil {
		return
	}

	c.KeepAliveTTL, err = time.ParseDuration(c.KeepAliveTTLStr)
	if err != nil {
		return
	}

	c.RPCTimeout, err = time.ParseDuration(c.RPCTimeoutStr)
	if err != nil {
		return
	}

	if _, err := label.NewSetFromMap(c.Labels); err != nil {
		return err
	}

	return nil
}

// GetDefaultExecutorConfig returns a default executor config
func GetDefaultExecutorConfig() *Config {
	return &Config{
		LogConf: logutil.Config{
			Level: "info",
			File:  "",
		},
		Name:                 "",
		Join:                 defaultJoinAddr,
		Addr:                 defaultExecutorAddr,
		AdvertiseAddr:        "",
		EnableGCTuning:       true, // currently 1 container 1 executor
		KeepAliveTTLStr:      defaultKeepAliveTTL,
		KeepAliveIntervalStr: defaultKeepAliveInterval,
		RPCTimeoutStr:        defaultRPCTimeout,
	}
}
