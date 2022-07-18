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

package master

import (
	"context"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/engine/pkg/cmd/util"
	"github.com/pingcap/tiflow/engine/pkg/version"
	"github.com/pingcap/tiflow/engine/servermaster"
	cmdconetxt "github.com/pingcap/tiflow/pkg/cmd/context"
	ticdcutil "github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/pingcap/tiflow/pkg/security"
)

// options defines flags for the `server` command.
type options struct {
	masterConfig         *servermaster.Config
	masterConfigFilePath string

	caPath        string
	certPath      string
	keyPath       string
	allowedCertCN string
}

// newOptions creates new options for the `server` command.
func newOptions() *options {
	return &options{
		masterConfig: servermaster.GetDefaultMasterConfig(),
	}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *options) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&o.masterConfig.MasterAddr, "master-addr", o.masterConfig.MasterAddr, "Set the listening address for server master")
	cmd.Flags().StringVar(&o.masterConfig.AdvertiseAddr, "advertise-addr", o.masterConfig.AdvertiseAddr, "Set the advertise listening address for client communication")

	cmd.Flags().StringVar(&o.masterConfig.LogConf.File, "log-file", o.masterConfig.LogConf.File, "log file path")
	cmd.Flags().StringVar(&o.masterConfig.LogConf.Level, "log-level", o.masterConfig.LogConf.Level, "log level (etc: debug|info|warn|error)")

	cmd.Flags().StringVar(&o.masterConfig.Etcd.Name, "name", o.masterConfig.Etcd.Name, "human readable name for server master")
	cmd.Flags().StringVar(&o.masterConfig.Etcd.DataDir, "data-dir", o.masterConfig.Etcd.DataDir, "data directory used for embed etcd")
	cmd.Flags().StringVar(&o.masterConfig.Etcd.InitialCluster, "initial-cluster", o.masterConfig.Etcd.InitialCluster, "initial cluster configuration for etcd bootstrapping")
	cmd.Flags().StringVar(&o.masterConfig.Etcd.PeerUrls, "peer-urls", o.masterConfig.Etcd.PeerUrls, "URLs for etcd peer traffic")
	cmd.Flags().StringVar(&o.masterConfig.Etcd.AdvertisePeerUrls, "advertise-peer-urls", o.masterConfig.Etcd.AdvertisePeerUrls, "advertise URLs for etcd peer traffic")

	cmd.Flags().StringSliceVar(&o.masterConfig.FrameMetaConf.Endpoints, "frame-meta-endpoints", o.masterConfig.FrameMetaConf.Endpoints, "framework metastore endpoint")
	cmd.Flags().StringVar(&o.masterConfig.FrameMetaConf.Auth.User, "frame-meta-user", o.masterConfig.FrameMetaConf.Auth.User, "framework metastore user")
	cmd.Flags().StringVar(&o.masterConfig.FrameMetaConf.Auth.Passwd, "frame-meta-password", o.masterConfig.FrameMetaConf.Auth.Passwd, "framework metastore password")
	cmd.Flags().StringVar(&o.masterConfig.FrameMetaConf.Schema, "frame-meta-schema", o.masterConfig.FrameMetaConf.Schema, `schema name for framework meta`)

	cmd.Flags().StringSliceVar(&o.masterConfig.BusinessMetaConf.Endpoints, "business-meta-endpoints", o.masterConfig.BusinessMetaConf.Endpoints, "business metastore endpoint")
	cmd.Flags().StringVar(&o.masterConfig.BusinessMetaConf.StoreType, "business-meta-store-type", o.masterConfig.BusinessMetaConf.StoreType, "business metastore store type")

	cmd.Flags().StringVar(&o.masterConfigFilePath, "config", "", "Path of the configuration file")

	cmd.Flags().StringVar(&o.caPath, "ca", "", "CA certificate path for TLS connection")
	cmd.Flags().StringVar(&o.certPath, "cert", "", "Certificate path for TLS connection")
	cmd.Flags().StringVar(&o.keyPath, "key", "", "Private key path for TLS connection")
	cmd.Flags().StringVar(&o.allowedCertCN, "cert-allowed-cn", "", "Verify caller's identity (cert Common Name). Use ',' to separate multiple CN")
}

// run runs the server cmd.
func (o *options) run(cmd *cobra.Command) error {
	err := logutil.InitLogger(&o.masterConfig.LogConf)
	if err != nil {
		return errors.Trace(err)
	}

	version.LogVersionInfo()
	if os.Getenv(gin.EnvGinMode) == "" {
		gin.SetMode(gin.ReleaseMode)
	}

	cancel := util.InitCmd(cmd)
	defer cancel()

	ticdcutil.LogHTTPProxies()

	server, err := servermaster.NewServer(o.masterConfig, nil)
	if err != nil {
		return errors.Trace(err)
	}

	err = server.Run(cmdconetxt.GetDefaultContext())
	if err != nil && errors.Cause(err) != context.Canceled {
		log.Error("run dataflow server master with error", zap.Error(err))
		return errors.Trace(err)
	}
	log.Info("dataflow server master exits successfully")

	return nil
}

// complete adapts from the command line args and config file to the data required.
func (o *options) complete(cmd *cobra.Command) error {
	o.masterConfig.Security = o.getCredential()

	cfg := servermaster.GetDefaultMasterConfig()

	if len(o.masterConfigFilePath) > 0 {
		if err := ticdcutil.StrictDecodeFile(
			o.masterConfigFilePath, "dataflow engine server master", cfg); err != nil {
			return err
		}
	}

	cmd.Flags().Visit(func(flag *pflag.Flag) {
		switch flag.Name {
		case "master-addr":
			cfg.MasterAddr = o.masterConfig.MasterAddr
		case "advertise-addr":
			cfg.AdvertiseAddr = o.masterConfig.AdvertiseAddr
		case "log-file":
			cfg.LogConf.File = o.masterConfig.LogConf.File
		case "log-level":
			cfg.LogConf.Level = o.masterConfig.LogConf.Level
		case "name":
			cfg.Etcd.Name = o.masterConfig.Etcd.Name
		case "data-dir":
			cfg.Etcd.DataDir = o.masterConfig.Etcd.DataDir
		case "initial-cluster":
			cfg.Etcd.InitialCluster = o.masterConfig.Etcd.InitialCluster
		case "peer-urls":
			cfg.Etcd.PeerUrls = o.masterConfig.Etcd.PeerUrls
		case "advertise-peer-urls":
			cfg.Etcd.AdvertisePeerUrls = o.masterConfig.Etcd.AdvertisePeerUrls
		case "frame-meta-endpoints":
			cfg.FrameMetaConf.Endpoints = o.masterConfig.FrameMetaConf.Endpoints
		case "frame-meta-user":
			cfg.FrameMetaConf.Auth.User = o.masterConfig.FrameMetaConf.Auth.User
		case "frame-meta-password":
			cfg.FrameMetaConf.Auth.Passwd = o.masterConfig.FrameMetaConf.Auth.Passwd
		case "business-meta-endpoints":
			cfg.BusinessMetaConf.Endpoints = o.masterConfig.BusinessMetaConf.Endpoints
		case "ca":
			cfg.Security.CAPath = o.masterConfig.Security.CAPath
		case "cert":
			cfg.Security.CertPath = o.masterConfig.Security.CertPath
		case "key":
			cfg.Security.KeyPath = o.masterConfig.Security.KeyPath
		case "cert-allowed-cn":
			cfg.Security.CertAllowedCN = o.masterConfig.Security.CertAllowedCN
		case "config":
			// do nothing
		default:
			log.Panic("unknown flag, please report a bug", zap.String("flagName", flag.Name))
		}
	})

	if err := cfg.Adjust(); err != nil {
		return errors.Trace(err)
	}

	o.masterConfig = cfg

	return nil
}

// getCredential returns security credential.
func (o *options) getCredential() *security.Credential {
	var certAllowedCN []string
	if len(o.allowedCertCN) != 0 {
		certAllowedCN = strings.Split(o.allowedCertCN, ",")
	}

	return &security.Credential{
		CAPath:        o.caPath,
		CertPath:      o.certPath,
		KeyPath:       o.keyPath,
		CertAllowedCN: certAllowedCN,
	}
}

// NewCmdMaster creates the `master` command.
func NewCmdMaster() *cobra.Command {
	o := newOptions()

	command := &cobra.Command{
		Use:   "master",
		Short: "Start a dataflow engine server master",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			err := o.complete(cmd)
			if err != nil {
				return err
			}
			err = o.run(cmd)
			cobra.CheckErr(err)
			return nil
		},
	}

	o.addFlags(command)

	return command
}
