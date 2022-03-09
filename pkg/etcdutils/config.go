package etcdutils

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

type ConfigParams struct {
	// etcd relative config items
	// NOTE: we use `Addr` to generate `ClientUrls` and `AdvertiseClientUrls`
	// NOTE: more items will be add when adding leader election
	Name                string `toml:"name" json:"name"`
	DataDir             string `toml:"data-dir" json:"data-dir"`
	PeerUrls            string `toml:"peer-urls" json:"peer-urls"`
	AdvertisePeerUrls   string `toml:"advertise-peer-urls" json:"advertise-peer-urls"`
	InitialCluster      string `toml:"initial-cluster" json:"initial-cluster"`
	InitialClusterState string `toml:"initial-cluster-state" json:"initial-cluster-state"`
	Join                string `toml:"join" json:"join"`
}

func (cp *ConfigParams) Adjust(
	defaultPeerUrls string,
	defaultInitialClusterState string,
) *ConfigParams {
	if cp.PeerUrls == "" {
		cp.PeerUrls = defaultPeerUrls
	}
	if cp.AdvertisePeerUrls == "" {
		cp.AdvertisePeerUrls = cp.PeerUrls
	}
	if cp.InitialCluster == "" {
		items := strings.Split(cp.AdvertisePeerUrls, ",")
		for i, item := range items {
			items[i] = fmt.Sprintf("%s=%s", cp.Name, item)
		}
		cp.InitialCluster = strings.Join(items, ",")
	}

	if cp.InitialClusterState == "" {
		cp.InitialClusterState = defaultInitialClusterState
	}
	return cp
}

// GenEmbedEtcdConfig generates the configuration needed by embed etcd.
// This method should be called after logger initialized and before any concurrent gRPC calls.
func GenEmbedEtcdConfig(
	cfg *embed.Config,
	addrs string,
	advertiseAddrs string,
	cp *ConfigParams,
) (*embed.Config, error) {
	cfg.Name = cp.Name
	cfg.Dir = cp.DataDir

	// reuse the provided addrs as the client listening URL.
	var err error
	cfg.LCUrls, err = parseURLs(addrs)
	if err != nil {
		return nil, errors.Wrap(errors.ErrMasterGenEmbedEtcdConfigFail, err, "invalid master-addr")
	}
	cfg.ACUrls, err = parseURLs(advertiseAddrs)
	if err != nil {
		return nil, errors.Wrap(errors.ErrMasterGenEmbedEtcdConfigFail, err, "invalid advertise-addr")
	}

	cfg.LPUrls, err = parseURLs(cp.PeerUrls)
	if err != nil {
		return nil, errors.Wrap(errors.ErrMasterGenEmbedEtcdConfigFail, err, "invalid peer-urls")
	}

	cfg.APUrls, err = parseURLs(cp.AdvertisePeerUrls)
	if err != nil {
		return nil, errors.Wrap(errors.ErrMasterGenEmbedEtcdConfigFail, err, "invalid advertise-peer-urls")
	}

	cfg.InitialCluster = cp.InitialCluster
	cfg.ClusterState = cp.InitialClusterState
	err = cfg.Validate() // verify & trigger the builder
	if err != nil {
		return nil, errors.Wrap(errors.ErrMasterGenEmbedEtcdConfigFail, err, "fail to validate embed etcd config")
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
			item = prefix + item
		}
		u, err := url.Parse(item)
		if err != nil {
			return nil, errors.Wrap(errors.ErrMasterParseURLFail, err, item)
		}
		if strings.Index(u.Host, ":") == 0 {
			u.Host = "0.0.0.0" + u.Host
		}
		urls = append(urls, *u)
	}
	return urls, nil
}

// GenEmbedEtcdConfigWithLogger creates embed.Config with given log level
func GenEmbedEtcdConfigWithLogger(logLevel string) *embed.Config {
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
