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

package executor

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/failpoint"

	"github.com/hanfei1991/microcosom/pkg/log"
	"github.com/hanfei1991/microcosom/pkg/terror"
)

// SampleConfigFile is sample config file of dm-worker.
var SampleConfigFile string

var (
	defaultKeepAliveTTL      = int64(60)      // 1 minute
	defaultRelayKeepAliveTTL = int64(60 * 30) // 30 minutes
)

func init() {
	failpoint.Inject("defaultKeepAliveTTL", func(val failpoint.Value) {
		i := val.(int)
		defaultKeepAliveTTL = int64(i)
	})
	failpoint.Inject("defaultRelayKeepAliveTTL", func(val failpoint.Value) {
		i := val.(int)
		defaultRelayKeepAliveTTL = int64(i)
	})
}

// NewConfig creates a new base config for worker.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.flagSet = flag.NewFlagSet("worker", flag.ContinueOnError)
	fs := cfg.flagSet

	fs.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")
	fs.BoolVar(&cfg.printSampleConfig, "print-sample-config", false, "print sample config file of dm-worker")
	fs.StringVar(&cfg.ConfigFile, "config", "", "path to config file")
	fs.StringVar(&cfg.WorkerAddr, "worker-addr", "", "listen address for client traffic")
	fs.StringVar(&cfg.AdvertiseAddr, "advertise-addr", "", `advertise address for client traffic (default "${worker-addr}")`)
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")
	fs.StringVar(&cfg.LogFormat, "log-format", "text", `the format of the log, "text" or "json"`)
	// fs.StringVar(&cfg.LogRotate, "log-rotate", "day", "log file rotate type, hour/day")
	// NOTE: add `advertise-addr` for dm-master if needed.
	fs.StringVar(&cfg.Join, "join", "", `join to an existing cluster (usage: dm-master cluster's "${master-addr}")`)
	fs.StringVar(&cfg.Name, "name", "", "human-readable name for DM-worker member")
	fs.Int64Var(&cfg.KeepAliveTTL, "keepalive-ttl", defaultKeepAliveTTL, "dm-worker's TTL for keepalive with etcd (in seconds)")

	return cfg
}

// Config is the configuration.
type Config struct {
	flagSet *flag.FlagSet
	Name    string `toml:"name" json:"name"`

	LogLevel  string `toml:"log-level" json:"log-level"`
	LogFile   string `toml:"log-file" json:"log-file"`
	LogFormat string `toml:"log-format" json:"log-format"`
	LogRotate string `toml:"log-rotate" json:"log-rotate"`

	Join          string `toml:"join" json:"join" `
	WorkerAddr    string `toml:"worker-addr" json:"worker-addr"`
	AdvertiseAddr string `toml:"advertise-addr" json:"advertise-addr"`

	ConfigFile string `toml:"config-file" json:"config-file"`
	// TODO: in the future dm-workers should share a same ttl from dm-master
	KeepAliveTTL      int64 `toml:"keepalive-ttl" json:"keepalive-ttl"`
	KeepAliveInterval int64 `toml:"keepalive-interval" json:"keepalive-interval"`

	printVersion      bool
	printSampleConfig bool
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
		log.L().Error("fail to marshal config to json", log.ShortError(err))
	}
	return string(cfg)
}

// Toml returns TOML format representation of config.
func (c *Config) Toml() (string, error) {
	var b bytes.Buffer

	err := toml.NewEncoder(&b).Encode(c)
	if err != nil {
		log.L().Error("fail to marshal config to toml", log.ShortError(err))
	}

	return b.String(), nil
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.flagSet.Parse(arguments)
	if err != nil {
		return terror.ErrWorkerParseFlagSet.Delegate(err)
	}

	if c.printSampleConfig {
		fmt.Println(SampleConfigFile)
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
		return terror.ErrWorkerParseFlagSet.Delegate(err)
	}

	if len(c.flagSet.Args()) != 0 {
		return terror.ErrWorkerInvalidFlag.Generate(c.flagSet.Arg(0))
	}

	return nil
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	metaData, err := toml.DecodeFile(path, c)
	if err != nil {
		return terror.ErrWorkerDecodeConfigFromFile.Delegate(err)
	}
	undecoded := metaData.Undecoded()
	if len(undecoded) > 0 && err == nil {
		var undecodedItems []string
		for _, item := range undecoded {
			undecodedItems = append(undecodedItems, item.String())
		}
		return terror.ErrWorkerUndecodedItemFromFile.Generate(strings.Join(undecodedItems, ","))
	}
	return nil
}
