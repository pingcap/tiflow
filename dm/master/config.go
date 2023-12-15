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
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/tiflow/dm/config/security"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/pkg/version"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
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

// SampleConfig is sample config of dm-master.
//
//go:embed dm-master.toml
var SampleConfig string

// NewConfig creates a config for dm-master.
func NewConfig() *Config {
	cfg := &Config{}
	cfg.flagSet = flag.NewFlagSet("dm-master", flag.ContinueOnError)
	fs := cfg.flagSet

	fs.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")
	fs.BoolVar(&cfg.printSampleConfig, "print-sample-config", false, "print sample config file of dm-worker")
	fs.BoolVar(&cfg.OpenAPI, "openapi", false, "enable openapi")
	fs.StringVar(&cfg.ConfigFile, "config", "", "path to config file")
	fs.StringVar(&cfg.MasterAddr, "master-addr", "", "master API server and status addr")
	fs.StringVar(&cfg.AdvertiseAddr, "advertise-addr", "", `advertise address for client traffic (default "${master-addr}")`)
	fs.StringVar(&cfg.LogLevel, "L", "info", "log level: debug, info, warn, error, fatal")
	fs.StringVar(&cfg.LogFile, "log-file", "", "log file path")
	fs.StringVar(&cfg.LogFormat, "log-format", "text", `the format of the log, "text" or "json"`)
	// fs.StringVar(&cfg.LogRotate, "log-rotate", "day", "log file rotate type, hour/day")

	fs.StringVar(&cfg.Name, "name", "", "human-readable name for this DM-master member")
	fs.StringVar(&cfg.DataDir, "data-dir", "", `path to the data directory (default "default.${name}")`)
	fs.StringVar(&cfg.InitialCluster, "initial-cluster", "", fmt.Sprintf("initial cluster configuration for bootstrapping, e.g. dm-master=%s", defaultPeerUrls))
	fs.StringVar(&cfg.PeerUrls, "peer-urls", defaultPeerUrls, "URLs for peer traffic")
	fs.StringVar(&cfg.AdvertisePeerUrls, "advertise-peer-urls", "", `advertise URLs for peer traffic (default "${peer-urls}")`)
	fs.StringVar(&cfg.Join, "join", "", `join to an existing cluster (usage: cluster's "${master-addr}" list, e.g. "127.0.0.1:8261,127.0.0.1:18261"`)
	fs.UintVar(&cfg.MaxTxnOps, "max-txn-ops", defaultMaxTxnOps, `etcd's max-txn-ops, default value is 2048`)
	fs.UintVar(&cfg.MaxRequestBytes, "max-request-bytes", embed.DefaultMaxRequestBytes, `etcd's max-request-bytes`)
	fs.StringVar(&cfg.AutoCompactionMode, "auto-compaction-mode", defaultAutoCompactionMode, `etcd's auto-compaction-mode, either 'periodic' or 'revision'`)
	fs.StringVar(&cfg.AutoCompactionRetention, "auto-compaction-retention", defaultAutoCompactionRetention, `etcd's auto-compaction-retention, accept values like '5h' or '5' (5 hours in 'periodic' mode or 5 revisions in 'revision')`)
	fs.Int64Var(&cfg.QuotaBackendBytes, "quota-backend-bytes", defaultQuotaBackendBytes, `etcd's storage quota in bytes`)

	fs.StringVar(&cfg.SSLCA, "ssl-ca", "", "path of file that contains list of trusted SSL CAs for connection")
	fs.StringVar(&cfg.SSLCert, "ssl-cert", "", "path of file that contains X509 certificate in PEM format for connection")
	fs.StringVar(&cfg.SSLKey, "ssl-key", "", "path of file that contains X509 key in PEM format for connection")
	fs.Var(&cfg.CertAllowedCN, "cert-allowed-cn", "the trusted common name that allowed to visit")

	fs.StringVar(&cfg.V1SourcesPath, "v1-sources-path", "", "directory path used to store source config files when upgrading from v1.0.x")

	return cfg
}

type ExperimentalFeatures struct {
	OpenAPI bool `toml:"openapi,omitempty"` // OpenAPI is available in v5.4 as default.
}

// Config is the configuration for dm-master.
type Config struct {
	flagSet *flag.FlagSet

	LogLevel  string `toml:"log-level" json:"log-level"`
	LogFile   string `toml:"log-file" json:"log-file"`
	LogFormat string `toml:"log-format" json:"log-format"`
	LogRotate string `toml:"log-rotate" json:"log-rotate"`

	RPCTimeoutStr string        `toml:"rpc-timeout" json:"rpc-timeout"`
	RPCTimeout    time.Duration `toml:"-" json:"-"`
	RPCRateLimit  float64       `toml:"rpc-rate-limit" json:"rpc-rate-limit"`
	RPCRateBurst  int           `toml:"rpc-rate-burst" json:"rpc-rate-burst"`

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
	InitialClusterState     string `toml:"initial-cluster-state" json:"initial-cluster-state"`
	Join                    string `toml:"join" json:"join"` // cluster's client address (endpoints), not peer address
	MaxTxnOps               uint   `toml:"max-txn-ops" json:"max-txn-ops"`
	MaxRequestBytes         uint   `toml:"max-request-bytes" json:"max-request-bytes"`
	AutoCompactionMode      string `toml:"auto-compaction-mode" json:"auto-compaction-mode"`
	AutoCompactionRetention string `toml:"auto-compaction-retention" json:"auto-compaction-retention"`
	QuotaBackendBytes       int64  `toml:"quota-backend-bytes" json:"quota-backend-bytes"`
	OpenAPI                 bool   `toml:"openapi" json:"openapi"`

	// directory path used to store source config files when upgrading from v1.0.x.
	// if this path set, DM-master leader will try to upgrade from v1.0.x to the current version.
	V1SourcesPath string `toml:"v1-sources-path" json:"v1-sources-path"`

	// tls config
	security.Security

	printVersion      bool
	printSampleConfig bool

	ExperimentalFeatures ExperimentalFeatures `toml:"experimental"`
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

	if c.printVersion {
		fmt.Println(version.GetRawInfo())
		return flag.ErrHelp
	}

	if c.printSampleConfig {
		fmt.Println(SampleConfig)
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

	return c.adjust()
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

// FromContent loads config from TOML format content.
func (c *Config) FromContent(content string) error {
	metaData, err := toml.Decode(content, c)
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
	return c.adjust()
}

// adjust adjusts configs.
func (c *Config) adjust() error {
	c.MasterAddr = utils.UnwrapScheme(c.MasterAddr)
	// MasterAddr's format may be "host:port" or ":port"
	host, port, err := net.SplitHostPort(c.MasterAddr)
	if err != nil {
		return terror.ErrMasterHostPortNotValid.Delegate(err, c.MasterAddr)
	}

	if c.AdvertiseAddr == "" {
		if host == "" || host == "0.0.0.0" || len(port) == 0 {
			return terror.ErrMasterHostPortNotValid.Generatef("master-addr (%s) must include the 'host' part (should not be '0.0.0.0') when advertise-addr is not set", c.MasterAddr)
		}
		c.AdvertiseAddr = c.MasterAddr
	} else {
		c.AdvertiseAddr = utils.UnwrapScheme(c.AdvertiseAddr)
		// AdvertiseAddr's format should be "host:port"
		host, port, err = net.SplitHostPort(c.AdvertiseAddr)
		if err != nil {
			return terror.ErrMasterAdvertiseAddrNotValid.Delegate(err, c.AdvertiseAddr)
		}
		if len(host) == 0 || host == "0.0.0.0" || len(port) == 0 {
			return terror.ErrMasterAdvertiseAddrNotValid.Generate(c.AdvertiseAddr)
		}
	}

	if c.RPCTimeoutStr == "" {
		c.RPCTimeoutStr = defaultRPCTimeout
	}
	timeout, err := time.ParseDuration(c.RPCTimeoutStr)
	if err != nil {
		return terror.ErrMasterConfigTimeoutParse.Delegate(err)
	}
	c.RPCTimeout = timeout

	// for backward compatibility
	if c.RPCRateLimit <= 0 {
		log.L().Warn("invalid rpc-rate-limit, default value used", zap.Float64("specified rpc-rate-limit", c.RPCRateLimit), zap.Float64("default rpc-rate-limit", DefaultRate))
		c.RPCRateLimit = DefaultRate
	}
	if c.RPCRateBurst <= 0 {
		log.L().Warn("invalid rpc-rate-burst, default value use", zap.Int("specified rpc-rate-burst", c.RPCRateBurst), zap.Int("default rpc-rate-burst", DefaultBurst))
		c.RPCRateBurst = DefaultBurst
	}

	if c.Name == "" {
		var hostname string
		hostname, err = os.Hostname()
		if err != nil {
			return terror.ErrMasterGetHostnameFail.Delegate(err)
		}
		c.Name = fmt.Sprintf("%s-%s", defaultNamePrefix, hostname)
	}

	if c.DataDir == "" {
		c.DataDir = fmt.Sprintf("%s.%s", defaultDataDirPrefix, c.Name)
	}

	if c.PeerUrls == "" {
		c.PeerUrls = defaultPeerUrls
	} else {
		c.PeerUrls = utils.WrapSchemes(c.PeerUrls, c.SSLCA != "")
	}

	if c.AdvertisePeerUrls == "" {
		c.AdvertisePeerUrls = c.PeerUrls
	} else {
		c.AdvertisePeerUrls = utils.WrapSchemes(c.AdvertisePeerUrls, c.SSLCA != "")
	}

	if c.InitialCluster == "" {
		items := strings.Split(c.AdvertisePeerUrls, ",")
		for i, item := range items {
			items[i] = fmt.Sprintf("%s=%s", c.Name, item)
		}
		c.InitialCluster = strings.Join(items, ",")
	} else {
		c.InitialCluster = utils.WrapSchemesForInitialCluster(c.InitialCluster, c.SSLCA != "")
	}

	if c.InitialClusterState == "" {
		c.InitialClusterState = defaultInitialClusterState
	}

	if c.Join != "" {
		c.Join = utils.WrapSchemes(c.Join, c.SSLCA != "")
	}

	if c.QuotaBackendBytes < quotaBackendBytesLowerBound {
		log.L().Warn("quota-backend-bytes is too low, will adjust it",
			zap.Int64("from", c.QuotaBackendBytes),
			zap.Int64("to", quotaBackendBytesLowerBound))
		c.QuotaBackendBytes = quotaBackendBytesLowerBound
	}

	if c.ExperimentalFeatures.OpenAPI {
		c.OpenAPI = true
		c.ExperimentalFeatures.OpenAPI = false
		log.L().Warn("openapi is a GA feature and removed from experimental features, so this configuration may have no affect in feature release, please set openapi=true in dm-master config file")
	}
	return err
}

// Reload load config from local file.
func (c *Config) Reload() error {
	if c.ConfigFile != "" {
		err := c.configFromFile(c.ConfigFile)
		if err != nil {
			return err
		}
	}

	return c.adjust()
}

// genEmbedEtcdConfig generates the configuration needed by embed etcd.
// This method should be called after logger initialized and before any concurrent gRPC calls.
func (c *Config) genEmbedEtcdConfig(cfg *embed.Config) (*embed.Config, error) {
	cfg.Name = c.Name
	cfg.Dir = c.DataDir

	// reuse the previous master-addr as the client listening URL.
	var err error
	cfg.ListenClientUrls, err = parseURLs(c.MasterAddr)
	if err != nil {
		return nil, terror.ErrMasterGenEmbedEtcdConfigFail.Delegate(err, "invalid master-addr")
	}
	cfg.AdvertiseClientUrls, err = parseURLs(c.AdvertiseAddr)
	if err != nil {
		return nil, terror.ErrMasterGenEmbedEtcdConfigFail.Delegate(err, "invalid advertise-addr")
	}

	cfg.ListenPeerUrls, err = parseURLs(c.PeerUrls)
	if err != nil {
		return nil, terror.ErrMasterGenEmbedEtcdConfigFail.Delegate(err, "invalid peer-urls")
	}

	cfg.AdvertisePeerUrls, err = parseURLs(c.AdvertisePeerUrls)
	if err != nil {
		return nil, terror.ErrMasterGenEmbedEtcdConfigFail.Delegate(err, "invalid advertise-peer-urls")
	}

	cfg.InitialCluster = c.InitialCluster
	cfg.ClusterState = c.InitialClusterState
	cfg.AutoCompactionMode = c.AutoCompactionMode
	cfg.AutoCompactionRetention = c.AutoCompactionRetention
	cfg.QuotaBackendBytes = c.QuotaBackendBytes
	cfg.MaxTxnOps = c.MaxTxnOps
	cfg.MaxRequestBytes = c.MaxRequestBytes

	err = cfg.Validate() // verify & trigger the builder
	if err != nil {
		return nil, terror.ErrMasterGenEmbedEtcdConfigFail.AnnotateDelegate(err, "fail to validate embed etcd config")
	}

	// security config
	if len(c.SSLCA) != 0 {
		cfg.ClientTLSInfo.TrustedCAFile = c.SSLCA
		cfg.ClientTLSInfo.CertFile = c.SSLCert
		cfg.ClientTLSInfo.KeyFile = c.SSLKey

		cfg.PeerTLSInfo.TrustedCAFile = c.SSLCA
		cfg.PeerTLSInfo.CertFile = c.SSLCert
		cfg.PeerTLSInfo.KeyFile = c.SSLKey

		// NOTE: etcd only support one allowed CN
		if len(c.CertAllowedCN) > 0 {
			cfg.ClientTLSInfo.AllowedCN = c.CertAllowedCN[0]
			cfg.PeerTLSInfo.AllowedCN = c.CertAllowedCN[0]
			cfg.PeerTLSInfo.ClientCertAuth = len(c.SSLCA) != 0
			cfg.ClientTLSInfo.ClientCertAuth = len(c.SSLCA) != 0
		}
	}

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
			if useTLS.Load() {
				prefix = "https://"
			}
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
