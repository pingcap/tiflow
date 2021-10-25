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
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"strings"

	"github.com/BurntSushi/toml"
	"go.etcd.io/etcd/embed"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosom/pkg/log"
	"github.com/hanfei1991/microcosom/pkg/terror"
)

const (
	defaultRPCTimeout              = "30s"
	defaultNamePrefix              = "dm-master"
	defaultDataDirPrefix           = "default"
	defaultPeerUrls                = "http://127.0.0.1:8291"
	defaultInitialClusterState     = embed.ClusterStateFlagNew
	defaultAutoCompactionMode      = "periodic"
	defaultAutoCompactionRetention = "1h"
	defaultMaxTxnOps               = 2048
	defaultQuotaBackendBytes       = 2 * 1024 * 1024 * 1024 // 2GB
	quotaBackendBytesLowerBound    = 500 * 1024 * 1024      // 500MB
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
	cfg := &Config{}
	cfg.flagSet = flag.NewFlagSet("dm-master", flag.ContinueOnError)
	fs := cfg.flagSet

	fs.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")
	fs.BoolVar(&cfg.printSampleConfig, "print-sample-config", false, "print sample config file of dm-worker")
	fs.StringVar(&cfg.ConfigFile, "config", "", "path to config file")
	fs.StringVar(&cfg.MasterAddr, "master-addr", "", "master API server and status addr")
	fs.StringVar(&cfg.AdvertiseAddr, "advertise-addr", "", `advertise address for client traffic (default "${master-addr}")`)
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")
	fs.StringVar(&cfg.LogFormat, "log-format", "text", `the format of the log, "text" or "json"`)
	// fs.StringVar(&cfg.LogRotate, "log-rotate", "day", "log file rotate type, hour/day")

	fs.StringVar(&cfg.Name, "name", "", "human-readable name for this DM-master member")
	fs.StringVar(&cfg.InitialCluster, "initial-cluster", "", fmt.Sprintf("initial cluster configuration for bootstrapping, e.g. dm-master=%s", defaultPeerUrls))
	fs.StringVar(&cfg.PeerUrls, "peer-urls", defaultPeerUrls, "URLs for peer traffic")
	fs.StringVar(&cfg.AdvertisePeerUrls, "advertise-peer-urls", "", `advertise URLs for peer traffic (default "${peer-urls}")`)


	return cfg
}

// Config is the configuration for dm-master.
type Config struct {
	flagSet *flag.FlagSet

	LogLevel  string `toml:"log-level" json:"log-level"`
	LogFile   string `toml:"log-file" json:"log-file"`
	LogFormat string `toml:"log-format" json:"log-format"`
	LogRotate string `toml:"log-rotate" json:"log-rotate"`

	MasterAddr    string `toml:"master-addr" json:"master-addr"`
	AdvertiseAddr string `toml:"advertise-addr" json:"advertise-addr"`

	ConfigFile string `toml:"config-file" json:"config-file"`

	// etcd relative config items
	// NOTE: we use `MasterAddr` to generate `ClientUrls` and `AdvertiseClientUrls`
	// NOTE: more items will be add when adding leader election
	Name                    string `toml:"name" json:"name"`
	DataDir                 string `toml:"data-dir" json:"data-dir"`
	PeerUrls                string `toml:"peer-urls" json:"peer-urls"`
	AdvertisePeerUrls       string `toml:"advertise-peer-urls" json:"advertise-peer-urls"`
	InitialCluster          string `toml:"initial-cluster" json:"initial-cluster"`
	Join                    string `toml:"join" json:"join"`

	printVersion      bool
	printSampleConfig bool
}

func (c *Config) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		log.L().Error("marshal to json", zap.Reflect("master config", c), log.ShortError(err))
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
		return terror.ErrMasterConfigParseFlagSet.Delegate(err)
	}

	if c.printSampleConfig {
		if strings.TrimSpace(SampleConfigFile) == "" {
			fmt.Println("sample config file of dm-master is empty")
		} else {
			rawConfig, err2 := base64.StdEncoding.DecodeString(SampleConfigFile)
			if err2 != nil {
				fmt.Println("base64 decode config error:", err2)
			} else {
				fmt.Println(string(rawConfig))
			}
		}
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
		return terror.ErrMasterConfigParseFlagSet.Delegate(err)
	}

	if len(c.flagSet.Args()) != 0 {
		return terror.ErrMasterConfigInvalidFlag.Generate(c.flagSet.Arg(0))
	}

	return nil
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	metaData, err := toml.DecodeFile(path, c)
	if err != nil {
		return terror.ErrMasterConfigTomlTransform.Delegate(err)
	}
	undecoded := metaData.Undecoded()
	if len(undecoded) > 0 {
		var undecodedItems []string
		for _, item := range undecoded {
			undecodedItems = append(undecodedItems, item.String())
		}
		return terror.ErrMasterConfigUnknownItem.Generate(strings.Join(undecodedItems, ","))
	}
	return nil
}


// genEmbedEtcdConfig generates the configuration needed by embed etcd.
// This method should be called after logger initialized and before any concurrent gRPC calls.
func (c *Config) genEmbedEtcdConfig(cfg *embed.Config) (*embed.Config, error) {
	cfg.Name = c.Name
	cfg.Dir = c.DataDir

	// reuse the previous master-addr as the client listening URL.
	var err error
	cfg.LCUrls, err = parseURLs(c.MasterAddr)
	if err != nil {
		return nil, terror.ErrMasterGenEmbedEtcdConfigFail.Delegate(err, "invalid master-addr")
	}
	cfg.ACUrls, err = parseURLs(c.AdvertiseAddr)
	if err != nil {
		return nil, terror.ErrMasterGenEmbedEtcdConfigFail.Delegate(err, "invalid advertise-addr")
	}

	cfg.LPUrls, err = parseURLs(c.PeerUrls)
	if err != nil {
		return nil, terror.ErrMasterGenEmbedEtcdConfigFail.Delegate(err, "invalid peer-urls")
	}

	cfg.APUrls, err = parseURLs(c.AdvertisePeerUrls)
	if err != nil {
		return nil, terror.ErrMasterGenEmbedEtcdConfigFail.Delegate(err, "invalid advertise-peer-urls")
	}

	cfg.InitialCluster = c.InitialCluster
	// TODO: To be simple, ignore them for now.
	//cfg.ClusterState = c.InitialClusterState
	//cfg.AutoCompactionMode = c.AutoCompactionMode
	//cfg.AutoCompactionRetention = c.AutoCompactionRetention
	//cfg.QuotaBackendBytes = c.QuotaBackendBytes
	//cfg.MaxTxnOps = c.MaxTxnOps
	//cfg.MaxRequestBytes = c.MaxRequestBytes

	err = cfg.Validate() // verify & trigger the builder
	if err != nil {
		return nil, terror.ErrMasterGenEmbedEtcdConfigFail.AnnotateDelegate(err, "fail to validate embed etcd config")
	}

	// security config
	//if len(c.SSLCA) != 0 {
	//	cfg.ClientTLSInfo.TrustedCAFile = c.SSLCA
	//	cfg.ClientTLSInfo.CertFile = c.SSLCert
	//	cfg.ClientTLSInfo.KeyFile = c.SSLKey

	//	cfg.PeerTLSInfo.TrustedCAFile = c.SSLCA
	//	cfg.PeerTLSInfo.CertFile = c.SSLCert
	//	cfg.PeerTLSInfo.KeyFile = c.SSLKey

	//	// NOTE: etcd only support one allowed CN
	//	if len(c.CertAllowedCN) > 0 {
	//		cfg.ClientTLSInfo.AllowedCN = c.CertAllowedCN[0]
	//		cfg.PeerTLSInfo.AllowedCN = c.CertAllowedCN[0]
	//		cfg.PeerTLSInfo.ClientCertAuth = len(c.SSLCA) != 0
	//		cfg.ClientTLSInfo.ClientCertAuth = len(c.SSLCA) != 0
	//	}
	//}

	return cfg, nil
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
			return nil, terror.ErrMasterParseURLFail.Delegate(err, item)
		}
		if strings.Index(u.Host, ":") == 0 {
			u.Host = "0.0.0.0" + u.Host
		}
		urls = append(urls, *u)
	}
	return urls, nil
}

func genEmbedEtcdConfigWithLogger(logLevel string) *embed.Config {
	cfg := embed.NewConfig()
	// disable grpc gateway because https://github.com/etcd-io/etcd/issues/12713
	// TODO: wait above issue fixed
	// cfg.EnableGRPCGateway = true // enable gRPC gateway for the internal etcd.

	// use zap as the logger for embed etcd
	// NOTE: `genEmbedEtcdConfig` can only be called after logger initialized.
	// NOTE: if using zap logger for etcd, must build it before any concurrent gRPC calls,
	// otherwise, DATA RACE occur in NewZapCoreLoggerBuilder and gRPC.
	logger := log.L().WithFields(zap.String("component", "embed etcd"))
	// if logLevel is info, set etcd log level to WARN to reduce log
	if strings.ToLower(logLevel) == "info" {
		log.L().Info("Set log level of etcd to `warn`, if you want to log more message about etcd, change log-level to `debug` in master configuration file")
		logger.Logger = logger.WithOptions(zap.IncreaseLevel(zap.WarnLevel))
	}

	cfg.ZapLoggerBuilder = embed.NewZapCoreLoggerBuilder(logger.Logger, logger.Core(), log.Props().Syncer) // use global app props.
	cfg.Logger = "zap"

	// TODO: we run ZapLoggerBuilder to set SetLoggerV2 before we do some etcd operations
	//       otherwise we will meet data race while running `grpclog.SetLoggerV2`
	//       It's vert tricky here, we should use a better way to avoid this in the future.
	err := cfg.ZapLoggerBuilder(cfg)
	if err != nil {
		panic(err) // we must ensure we can generate embed etcd config
	}

	return cfg
}
