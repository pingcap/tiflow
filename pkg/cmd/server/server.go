// Copyright 2021 PingCAP, Inc.
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

package server

import (
	"context"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	ticonfig "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tiflow/cdc/server"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/pingcap/tiflow/pkg/security"
	ticdcutil "github.com/pingcap/tiflow/pkg/util"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

// Options defines flags for the `server` command.
// Exported for the new architecture of TiCDC only.
type Options struct {
	ServerConfig         *config.ServerConfig
	ServerPdAddr         string
	ServerConfigFilePath string

	// TODO(hi-rustin): Consider using a client construction factory here.
	CaPath        string
	CertPath      string
	KeyPath       string
	AllowedCertCN string
}

// newOptions creates new options for the `server` command.
func newOptions() *Options {
	return &Options{
		ServerConfig: config.GetDefaultServerConfig(),
	}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *Options) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&o.ServerConfig.ClusterID, "cluster-id", "default", "Set cdc cluster id")
	cmd.Flags().StringVar(&o.ServerConfig.Addr, "addr", o.ServerConfig.Addr, "Set the listening address")
	cmd.Flags().StringVar(&o.ServerConfig.AdvertiseAddr, "advertise-addr", o.ServerConfig.AdvertiseAddr, "Set the advertise listening address for client communication")

	cmd.Flags().StringVar(&o.ServerConfig.TZ, "tz", o.ServerConfig.TZ, "Specify time zone of TiCDC cluster")
	cmd.Flags().Int64Var(&o.ServerConfig.GcTTL, "gc-ttl", o.ServerConfig.GcTTL, "CDC GC safepoint TTL duration, specified in seconds")

	cmd.Flags().StringVar(&o.ServerConfig.LogFile, "log-file", o.ServerConfig.LogFile, "log file path")
	cmd.Flags().StringVar(&o.ServerConfig.LogLevel, "log-level", o.ServerConfig.LogLevel, "log level (etc: debug|info|warn|error)")

	cmd.Flags().StringVar(&o.ServerConfig.DataDir, "data-dir", o.ServerConfig.DataDir, "the path to the directory used to store TiCDC-generated data")

	cmd.Flags().DurationVar((*time.Duration)(&o.ServerConfig.OwnerFlushInterval), "owner-flush-interval", time.Duration(o.ServerConfig.OwnerFlushInterval), "owner flushes changefeed status interval")
	_ = cmd.Flags().MarkHidden("owner-flush-interval")

	cmd.Flags().DurationVar((*time.Duration)(&o.ServerConfig.ProcessorFlushInterval), "processor-flush-interval", time.Duration(o.ServerConfig.ProcessorFlushInterval), "processor flushes task status interval")
	_ = cmd.Flags().MarkHidden("processor-flush-interval")

	// 80 is safe on most systems.
	// sort-dir id deprecate, hidden it.
	cmd.Flags().StringVar(&o.ServerConfig.Sorter.SortDir, "sort-dir", o.ServerConfig.Sorter.SortDir, "sorter's temporary file directory")
	_ = cmd.Flags().MarkHidden("sort-dir")

	cmd.Flags().StringVar(&o.ServerPdAddr, "pd", "http://127.0.0.1:2379", "Set the PD endpoints to use. Use ',' to separate multiple PDs")
	cmd.Flags().StringVar(&o.ServerConfigFilePath, "config", "", "Path of the configuration file")

	cmd.Flags().StringVar(&o.CaPath, "ca", "", "CA certificate path for TLS connection")
	cmd.Flags().StringVar(&o.CertPath, "cert", "", "Certificate path for TLS connection")
	cmd.Flags().StringVar(&o.KeyPath, "key", "", "Private key path for TLS connection")
	cmd.Flags().StringVar(&o.AllowedCertCN, "cert-allowed-cn", "", "Verify caller's identity (cert Common Name). Use ',' to separate multiple CN")
}

// run runs the server cmd.
func (o *Options) run(cmd *cobra.Command) error {
	cancel := util.InitCmd(cmd, &logutil.Config{
		File:                 o.ServerConfig.LogFile,
		Level:                o.ServerConfig.LogLevel,
		FileMaxSize:          o.ServerConfig.Log.File.MaxSize,
		FileMaxDays:          o.ServerConfig.Log.File.MaxDays,
		FileMaxBackups:       o.ServerConfig.Log.File.MaxBackups,
		ZapInternalErrOutput: o.ServerConfig.Log.InternalErrOutput,
	})
	defer cancel()

	_, err := ticdcutil.GetTimezone(o.ServerConfig.TZ)
	if err != nil {
		return errors.Annotate(err, "can not load timezone, Please specify the time zone through environment variable `TZ` or command line parameters `--tz`")
	}

	config.StoreGlobalServerConfig(o.ServerConfig)
	ctx := cmdcontext.GetDefaultContext()

	version.LogVersionInfo("Change Data Capture (CDC)")
	if ticdcutil.FailpointBuild {
		for _, path := range failpoint.List() {
			status, err := failpoint.Status(path)
			if err != nil {
				log.Error("fail to get failpoint status", zap.Error(err))
			}
			log.Info("failpoint enabled", zap.String("path", path), zap.String("status", status))
		}
	}

	util.LogHTTPProxies()
	server.RecordGoRuntimeSettings()
	server, err := server.New(strings.Split(o.ServerPdAddr, ","))
	if err != nil {
		log.Error("create cdc server failed", zap.Error(err))
		return errors.Trace(err)
	}
	// Drain the server before shutdown.
	util.InitSignalHandling(server.Drain, cancel)

	// Run TiCDC server.
	err = server.Run(ctx)
	if err != nil && errors.Cause(err) != context.Canceled {
		log.Warn("cdc server exits with error", zap.Error(err))
	} else {
		log.Info("cdc server exits normally")
	}
	server.Close()
	return nil
}

// complete adapts from the command line args and config file to the data required.
func (o *Options) complete(cmd *cobra.Command) error {
	o.ServerConfig.Security = o.getCredential()

	cfg := config.GetDefaultServerConfig()

	if len(o.ServerConfigFilePath) > 0 {
		// strict decode config file, but ignore debug and newarch item
		// the newarch item is only used in new ticdc
		if err := util.StrictDecodeFile(o.ServerConfigFilePath, "TiCDC server", cfg, config.DebugConfigurationItem, config.NewArchConfigurationItem); err != nil {
			return err
		}

		// User specified sort-dir should not take effect, it's always `/tmp/sorter`
		// if user try to set sort-dir by config file, warn it.
		if cfg.Sorter.SortDir != config.DefaultSortDir {
			cmd.Printf("%s", color.HiYellowString("[WARN] --sort-dir is deprecated in server settings. "+
				"sort-dir will be set to `{data-dir}/tmp/sorter`. The sort-dir here will be no-op\n"))

			cfg.Sorter.SortDir = config.DefaultSortDir
		}
	}

	cmd.Flags().Visit(func(flag *pflag.Flag) {
		switch flag.Name {
		case "addr":
			cfg.Addr = o.ServerConfig.Addr
		case "advertise-addr":
			cfg.AdvertiseAddr = o.ServerConfig.AdvertiseAddr
		case "tz":
			cfg.TZ = o.ServerConfig.TZ
		case "gc-ttl":
			cfg.GcTTL = o.ServerConfig.GcTTL
		case "log-file":
			cfg.LogFile = o.ServerConfig.LogFile
		case "log-level":
			cfg.LogLevel = o.ServerConfig.LogLevel
		case "data-dir":
			cfg.DataDir = o.ServerConfig.DataDir
		case "owner-flush-interval":
			cfg.OwnerFlushInterval = o.ServerConfig.OwnerFlushInterval
		case "processor-flush-interval":
			cfg.ProcessorFlushInterval = o.ServerConfig.ProcessorFlushInterval
		case "ca":
			cfg.Security.CAPath = o.ServerConfig.Security.CAPath
		case "cert":
			cfg.Security.CertPath = o.ServerConfig.Security.CertPath
		case "key":
			cfg.Security.KeyPath = o.ServerConfig.Security.KeyPath
		case "cert-allowed-cn":
			cfg.Security.CertAllowedCN = o.ServerConfig.Security.CertAllowedCN
		case "sort-dir":
			// user specified sorter dir should not take effect, it's always `/tmp/sorter`
			// if user try to set sort-dir by flag, warn it.
			if o.ServerConfig.Sorter.SortDir != config.DefaultSortDir {
				cmd.Printf("%s", color.HiYellowString("[WARN] --sort-dir is deprecated in server settings. "+
					"sort-dir will be set to `{data-dir}/tmp/sorter`. The sort-dir here will be no-op\n"))
			}
			cfg.Sorter.SortDir = config.DefaultSortDir
		case "cluster-id":
			cfg.ClusterID = o.ServerConfig.ClusterID
		case "pd", "config":
			// do nothing
		default:
			log.Panic("unknown flag, please report a bug", zap.String("flagName", flag.Name))
		}
	})

	if err := cfg.ValidateAndAdjust(); err != nil {
		return errors.Trace(err)
	}

	if cfg.DataDir == "" {
		cmd.Printf("%s", color.HiYellowString("[WARN] TiCDC server data-dir is not set. "+
			"Please use `cdc server --data-dir` to start the cdc server if possible.\n"))
	}

	o.ServerConfig = cfg

	return nil
}

// validate checks that the provided attach options are specified.
func (o *Options) validate() error {
	if len(o.ServerPdAddr) == 0 {
		return cerror.ErrInvalidServerOption.GenWithStack("empty PD address")
	}
	for _, ep := range strings.Split(o.ServerPdAddr, ",") {
		// NOTICE: The configuration used here is the one that has been completed,
		// as it may be configured by the configuration file.
		if err := util.VerifyPdEndpoint(ep, o.ServerConfig.Security.IsTLSEnabled()); err != nil {
			return cerror.WrapError(cerror.ErrInvalidServerOption, err)
		}
	}
	return nil
}

// getCredential returns security credential.
func (o *Options) getCredential() *security.Credential {
	var certAllowedCN []string
	if len(o.AllowedCertCN) != 0 {
		certAllowedCN = strings.Split(o.AllowedCertCN, ",")
	}

	return &security.Credential{
		CAPath:        o.CaPath,
		CertPath:      o.CertPath,
		KeyPath:       o.KeyPath,
		CertAllowedCN: certAllowedCN,
	}
}

// Run a TiCDC server.
// Exported for the new architecture of TiCDC only.
func Run(o *Options, cmd *cobra.Command) error {
	err := o.complete(cmd)
	if err != nil {
		return err
	}
	err = o.validate()
	if err != nil {
		return err
	}
	err = o.run(cmd)
	cobra.CheckErr(err)
	return nil
}

// NewCmdServer creates the `server` command.
func NewCmdServer() *cobra.Command {
	o := newOptions()

	command := &cobra.Command{
		Use:   "server",
		Short: "Start a TiCDC capture server",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return Run(o, cmd)
		},
	}

	patchTiDBConf()
	o.addFlags(command)

	return command
}

func patchTiDBConf() {
	ticonfig.UpdateGlobal(func(conf *ticonfig.Config) {
		// Disable kv client batch send loop introduced by tidb library, which is not used in TiCDC server
		conf.TiKVClient.MaxBatchSize = 0
	})
}
