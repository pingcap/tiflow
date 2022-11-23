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

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/pkg/cmd/util"
	"github.com/pingcap/tiflow/engine/servermaster"
	cmdconetxt "github.com/pingcap/tiflow/pkg/cmd/context"
	ticdcutil "github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

// options defines flags for the `server` command.
type options struct {
	masterConfig         *servermaster.Config
	masterConfigFilePath string
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
	cmd.Flags().StringVar(&o.masterConfig.Name, "name", o.masterConfig.Name, "human readable name for master")
	cmd.Flags().StringVar(&o.masterConfig.Addr, "addr", o.masterConfig.Addr, "Set the listening address for server master")
	cmd.Flags().StringVar(&o.masterConfig.AdvertiseAddr, "advertise-addr", o.masterConfig.AdvertiseAddr, "Set the advertise listening address for client communication")

	cmd.Flags().StringSliceVar(&o.masterConfig.FrameworkMeta.Endpoints, "framework-meta-endpoints", o.masterConfig.FrameworkMeta.Endpoints, "framework metastore endpoint")
	cmd.Flags().StringSliceVar(&o.masterConfig.BusinessMeta.Endpoints, "business-meta-endpoints", o.masterConfig.BusinessMeta.Endpoints, "business metastore endpoint")

	cmd.Flags().StringVar(&o.masterConfigFilePath, "config", "", "Path of the configuration file")
	cmd.Flags().StringVar(&o.masterConfig.LogConf.File, "log-file", o.masterConfig.LogConf.File, "log file path")
	cmd.Flags().StringVar(&o.masterConfig.LogConf.Level, "log-level", o.masterConfig.LogConf.Level, "log level (etc: debug|info|warn|error)")
}

// run runs the server cmd.
func (o *options) run(cmd *cobra.Command) error {
	err := logutil.InitLogger(&o.masterConfig.LogConf)
	if err != nil {
		return errors.Trace(err)
	}

	version.LogVersionInfo("TiFlow Master")
	if os.Getenv(gin.EnvGinMode) == "" {
		gin.SetMode(gin.ReleaseMode)
	}

	cancel := util.InitCmd(cmd)
	defer cancel()

	ticdcutil.LogHTTPProxies()

	server, err := servermaster.NewServer(o.masterConfig)
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
	cfg := servermaster.GetDefaultMasterConfig()

	if len(o.masterConfigFilePath) > 0 {
		if err := ticdcutil.StrictDecodeFile(
			o.masterConfigFilePath, "dataflow engine server master", cfg); err != nil {
			return err
		}
	}

	cmd.Flags().Visit(func(flag *pflag.Flag) {
		switch flag.Name {
		case "name":
			cfg.Name = o.masterConfig.Name
		case "addr":
			cfg.Addr = o.masterConfig.Addr
		case "advertise-addr":
			cfg.AdvertiseAddr = o.masterConfig.AdvertiseAddr
		case "framework-meta-endpoints":
			cfg.FrameworkMeta.Endpoints = o.masterConfig.FrameworkMeta.Endpoints
		case "business-meta-endpoints":
			cfg.BusinessMeta.Endpoints = o.masterConfig.BusinessMeta.Endpoints
		case "config":
			// do nothing
		case "log-file":
			cfg.LogConf.File = o.masterConfig.LogConf.File
		case "log-level":
			cfg.LogConf.Level = o.masterConfig.LogConf.Level
		default:
			log.Panic("unknown flag, please report a bug", zap.String("flagName", flag.Name))
		}
	})

	if err := cfg.AdjustAndValidate(); err != nil {
		return errors.Trace(err)
	}

	o.masterConfig = cfg

	return nil
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
