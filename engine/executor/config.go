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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/log"

	"github.com/pingcap/tiflow/engine/pkg/externalresource/storagecfg"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/pingcap/tiflow/pkg/security"
)

var (
	defaultSessionTTL        = 20
	defaultKeepAliveTTL      = "20s"
	defaultKeepAliveInterval = "500ms"
	defaultRPCTimeout        = "3s"
	defaultDiscoverTicker    = 3 * time.Second
	defaultMetricInterval    = 15 * time.Second

	defaultCapability            int64 = 100 // TODO: make this configurable
	defaultLocalStorageDirPrefix       = "/tmp/dfe-storage/"
)

// Config is the configuration.
type Config struct {
	// TODO: is executor name necessary, executor.Info.ID has similar effect
	Name string `toml:"name" json:"name"`

	LogConf logutil.Config `toml:"log" json:"log"`

	Join          string `toml:"join" json:"join" `
	WorkerAddr    string `toml:"worker-addr" json:"worker-addr"`
	AdvertiseAddr string `toml:"advertise-addr" json:"advertise-addr"`

	SessionTTL int `toml:"session-ttl" json:"session-ttl"`

	ConfigFile string `toml:"config-file" json:"config-file"`

	// TODO: in the future dm-workers should share a same ttl from dm-master
	KeepAliveTTLStr      string `toml:"keepalive-ttl" json:"keepalive-ttl"`
	KeepAliveIntervalStr string `toml:"keepalive-interval" json:"keepalive-interval"`
	RPCTimeoutStr        string `toml:"rpc-timeout" json:"rpc-timeout"`

	Storage storagecfg.Config `toml:"storage" json:"storage"`

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

func getDefaultLocalStorageDir(executorName string) string {
	// Use hex encoding in case there are special characters in the
	// executor name.
	encodedExecutorName := hex.EncodeToString([]byte(executorName))
	return filepath.Join(defaultLocalStorageDirPrefix, encodedExecutorName)
}

// Adjust adjusts the executor configuration
func (c *Config) Adjust() (err error) {
	if c.AdvertiseAddr == "" {
		c.AdvertiseAddr = c.WorkerAddr
	}

	if c.Name == "" {
		c.Name = fmt.Sprintf("executor-%s", c.AdvertiseAddr)
	}

	if c.Storage.Local.BaseDir == "" {
		c.Storage.Local.BaseDir = getDefaultLocalStorageDir(c.Name)
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

	return
}

// GetDefaultExecutorConfig returns a default executor config
func GetDefaultExecutorConfig() *Config {
	return &Config{
		LogConf: logutil.Config{
			Level: "info",
			File:  "",
		},
		Name:                 "",
		Join:                 "",
		WorkerAddr:           "",
		AdvertiseAddr:        "",
		SessionTTL:           defaultSessionTTL,
		KeepAliveTTLStr:      defaultKeepAliveTTL,
		KeepAliveIntervalStr: defaultKeepAliveInterval,
		RPCTimeoutStr:        defaultRPCTimeout,
		Storage: storagecfg.Config{
			Local: storagecfg.LocalFileConfig{BaseDir: ""},
		},
	}
}
