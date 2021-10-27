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

package worker

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/failpoint"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

// SampleConfigFile is sample config file of dm-worker.
//go:embed dm-worker.toml
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
	fs.Int64Var(&cfg.RelayKeepAliveTTL, "relay-keepalive-ttl", defaultRelayKeepAliveTTL, "dm-worker's TTL for keepalive with etcd when handle relay enabled sources (in seconds)")

	fs.StringVar(&cfg.SSLCA, "ssl-ca", "", "path of file that contains list of trusted SSL CAs for connection")
	fs.StringVar(&cfg.SSLCert, "ssl-cert", "", "path of file that contains X509 certificate in PEM format for connection")
	fs.StringVar(&cfg.SSLKey, "ssl-key", "", "path of file that contains X509 key in PEM format for connection")
	fs.Var(&cfg.CertAllowedCN, "cert-allowed-cn", "the trusted common name that allowed to visit")

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
	RelayKeepAliveTTL int64 `toml:"relay-keepalive-ttl" json:"relay-keepalive-ttl"`

	// tls config
	config.Security

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

	if c.printVersion {
		fmt.Println(utils.GetRawInfo())
		return flag.ErrHelp
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

	return c.adjust()
}

// adjust adjusts the config.
func (c *Config) adjust() error {
	c.WorkerAddr = utils.UnwrapScheme(c.WorkerAddr)
	host, _, err := net.SplitHostPort(c.WorkerAddr)
	if err != nil {
		return terror.ErrWorkerHostPortNotValid.Delegate(err, c.WorkerAddr)
	}

	if c.AdvertiseAddr == "" {
		if host == "" || host == "0.0.0.0" {
			return terror.ErrWorkerHostPortNotValid.Generatef("worker-addr (%s) must include the 'host' part (should not be '0.0.0.0') when advertise-addr is not set", c.WorkerAddr)
		}
		c.AdvertiseAddr = c.WorkerAddr
	} else {
		c.AdvertiseAddr = utils.UnwrapScheme(c.AdvertiseAddr)
		var port string
		host, port, err = net.SplitHostPort(c.AdvertiseAddr)
		if err != nil {
			return terror.ErrWorkerHostPortNotValid.Delegate(err, c.AdvertiseAddr)
		}
		if host == "" || host == "0.0.0.0" || len(port) == 0 {
			return terror.ErrWorkerHostPortNotValid.Generate("advertise-addr (%s) must include the 'host' part and should not be '0.0.0.0'", c.AdvertiseAddr)
		}
	}

	if c.Name == "" {
		fmt.Printf("worker name is not given, we will set AdvertiseAddr %s as the worker name\n", c.AdvertiseAddr)
		c.Name = c.AdvertiseAddr
	}

	if c.Join != "" {
		c.Join = utils.WrapSchemes(c.Join, c.SSLCA != "")
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
