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
	"context"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/engine/executor"
	"github.com/pingcap/tiflow/engine/pkg/cmd/util"
	"github.com/pingcap/tiflow/engine/pkg/version"
	cmdconetxt "github.com/pingcap/tiflow/pkg/cmd/context"
	ticdcutil "github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/pingcap/tiflow/pkg/security"
)

// options defines flags for the `server` command.
type options struct {
	executorConfig         *executor.Config
	executorConfigFilePath string

	caPath        string
	certPath      string
	keyPath       string
	allowedCertCN string
}

// newOptions creates new options for the `server` command.
func newOptions() *options {
	return &options{
		executorConfig: executor.GetDefaultExecutorConfig(),
	}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *options) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&o.executorConfig.WorkerAddr, "worker-addr", o.executorConfig.WorkerAddr, "Set the listening address for executor")
	cmd.Flags().StringVar(&o.executorConfig.AdvertiseAddr, "advertise-addr", o.executorConfig.AdvertiseAddr, "Set the advertise listening address for client communication")

	cmd.Flags().StringVar(&o.executorConfig.LogConf.File, "log-file", o.executorConfig.LogConf.File, "log file path")
	cmd.Flags().StringVar(&o.executorConfig.LogConf.Level, "log-level", o.executorConfig.LogConf.Level, "log level (etc: debug|info|warn|error)")

	cmd.Flags().StringVar(&o.executorConfig.Name, "name", o.executorConfig.Name, "human readable name for executor")
	cmd.Flags().StringVar(&o.executorConfig.Join, "join", o.executorConfig.Join, "join to an existing cluster (usage: server masters' address)")

	cmd.Flags().StringVar(&o.executorConfigFilePath, "config", "", "Path of the configuration file")

	cmd.Flags().StringVar(&o.caPath, "ca", "", "CA certificate path for TLS connection")
	cmd.Flags().StringVar(&o.certPath, "cert", "", "Certificate path for TLS connection")
	cmd.Flags().StringVar(&o.keyPath, "key", "", "Private key path for TLS connection")
	cmd.Flags().StringVar(&o.allowedCertCN, "cert-allowed-cn", "", "Verify caller's identity (cert Common Name). Use ',' to separate multiple CN")
}

// run runs the server cmd.
func (o *options) run(cmd *cobra.Command) error {
	err := logutil.InitLogger(&o.executorConfig.LogConf)
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

	server := executor.NewServer(o.executorConfig, nil)

	err = server.Run(cmdconetxt.GetDefaultContext())
	if err != nil && errors.Cause(err) != context.Canceled {
		log.Error("run dataflow executor with error", zap.Error(err))
		return errors.Trace(err)
	}
	log.Info("dataflow executor exits successfully")

	return nil
}

// complete adapts from the command line args and config file to the data required.
func (o *options) complete(cmd *cobra.Command) error {
	o.executorConfig.Security = o.getCredential()

	cfg := executor.GetDefaultExecutorConfig()

	if len(o.executorConfigFilePath) > 0 {
		if err := ticdcutil.StrictDecodeFile(
			o.executorConfigFilePath, "dataflow engine executor", cfg); err != nil {
			return err
		}
	}

	cmd.Flags().Visit(func(flag *pflag.Flag) {
		switch flag.Name {
		case "name":
			cfg.Name = o.executorConfig.Name
		case "worker-addr":
			cfg.WorkerAddr = o.executorConfig.WorkerAddr
		case "advertise-addr":
			cfg.AdvertiseAddr = o.executorConfig.AdvertiseAddr
		case "log-file":
			cfg.LogConf.File = o.executorConfig.LogConf.File
		case "log-level":
			cfg.LogConf.Level = o.executorConfig.LogConf.Level
		case "join":
			cfg.Join = o.executorConfig.Join
		case "ca":
			cfg.Security.CAPath = o.executorConfig.Security.CAPath
		case "cert":
			cfg.Security.CertPath = o.executorConfig.Security.CertPath
		case "key":
			cfg.Security.KeyPath = o.executorConfig.Security.KeyPath
		case "cert-allowed-cn":
			cfg.Security.CertAllowedCN = o.executorConfig.Security.CertAllowedCN
		case "config":
			// do nothing
		default:
			log.Panic("unknown flag, please report a bug", zap.String("flagName", flag.Name))
		}
	})

	if err := cfg.Adjust(); err != nil {
		return errors.Trace(err)
	}

	o.executorConfig = cfg

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

// NewCmdExecutor creates the `master` command.
func NewCmdExecutor() *cobra.Command {
	o := newOptions()

	command := &cobra.Command{
		Use:   "executor",
		Short: "Start a dataflow engine executor",
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
