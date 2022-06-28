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
	"flag"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/BurntSushi/toml"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/storagecfg"
	"github.com/pingcap/tiflow/engine/pkg/version"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/logutil"
)

// SampleConfigFile is sample config file of dm-worker.
var SampleConfigFile string

var (
	defaultKeepAliveTTL      = "20s"
	defaultKeepAliveInterval = "500ms"
	defaultRPCTimeout        = "3s"
	defaultDiscoverTicker    = 3 * time.Second
	defaultMetricInterval    = 15 * time.Second

	defaultCapability            int64 = 100 // TODO: make this configurable
	defaultLocalStorageDirPrefix       = "/tmp/dfe-storage/"
)

// NewConfig creates a new base config for worker.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.flagSet = flag.NewFlagSet("worker", flag.ContinueOnError)
	fs := cfg.flagSet

	fs.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")
	fs.StringVar(&cfg.ConfigFile, "config", "", "path to config file")
	fs.StringVar(&cfg.WorkerAddr, "worker-addr", "", "listen address for client traffic")
	fs.StringVar(&cfg.AdvertiseAddr, "advertise-addr", "", `advertise address for client traffic (default "${worker-addr}")`)
	fs.StringVar(&cfg.LogConf.Level, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogConf.File, "log-file", "", "log file path")
	// fs.StringVar(&cfg.LogConf.LogRotate, "log-rotate", "day", "log file rotate type, hour/day")
	// NOTE: add `advertise-addr` for dm-master if needed.
	fs.StringVar(&cfg.Join, "join", "", `join to an existing cluster (usage: server masters' address)`)
	fs.StringVar(&cfg.Name, "name", "", "human-readable name for executor")
	fs.StringVar(&cfg.KeepAliveTTLStr, "keepalive-ttl", defaultKeepAliveTTL, "executor's TTL for keepalive with etcd (in seconds)")

	return cfg
}

// Config is the configuration.
type Config struct {
	flagSet *flag.FlagSet
	Name    string `toml:"name" json:"name"`

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

	PollConcurrency int               `toml:"poll-concurrency" json:"poll-concurrency"`
	Storage         storagecfg.Config `toml:"storage" json:"storage"`

	KeepAliveTTL      time.Duration `toml:"-" json:"-"`
	KeepAliveInterval time.Duration `toml:"-" json:"-"`
	RPCTimeout        time.Duration `toml:"-" json:"-"`

	printVersion bool
}

// Clone clones a config.
func (c *Config) Clone() *Config {
	clone := &Config{}
	*clone = *c
	return clone
}

func (c *Config) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		log.L().Error("fail to marshal config to json", logutil.ShortError(err))
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
		return errors.WrapError(errors.ErrExecutorConfigParseFlagSet, err)
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
		return errors.WrapError(errors.ErrExecutorConfigParseFlagSet, err)
	}

	if len(c.flagSet.Args()) != 0 {
		return errors.ErrExecutorConfigInvalidFlag.GenWithStackByArgs(c.flagSet.Arg(0))
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
	if c.PollConcurrency == 0 {
		c.PollConcurrency = runtime.NumCPU()
	}

	if c.AdvertiseAddr == "" {
		c.AdvertiseAddr = c.WorkerAddr
	}

	if c.Name == "" {
		c.Name = fmt.Sprintf("executor-%s", c.AdvertiseAddr)
	}

	if c.Storage.Local.BaseDir == "" {
		c.Storage.Local.BaseDir = getDefaultLocalStorageDir(c.Name)
	}
	return nil
}

// configFromFile loads config from file.
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
