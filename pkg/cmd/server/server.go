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
	ticonfig "github.com/pingcap/tidb/config"
	"github.com/pingcap/tiflow/cdc"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/sorter/unified"
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

// options defines flags for the `server` command.
type options struct {
	serverConfig         *config.ServerConfig
	serverPdAddr         string
	serverConfigFilePath string

	// TODO(hi-rustin): Consider using a client construction factory here.
	caPath        string
	certPath      string
	keyPath       string
	allowedCertCN string
}

// newOptions creates new options for the `server` command.
func newOptions() *options {
	return &options{
		serverConfig: config.GetDefaultServerConfig(),
	}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *options) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&o.serverConfig.Addr, "addr", o.serverConfig.Addr, "Set the listening address")
	cmd.Flags().StringVar(&o.serverConfig.AdvertiseAddr, "advertise-addr", o.serverConfig.AdvertiseAddr, "Set the advertise listening address for client communication")

	cmd.Flags().StringVar(&o.serverConfig.TZ, "tz", o.serverConfig.TZ, "Specify time zone of TiCDC cluster")
	cmd.Flags().Int64Var(&o.serverConfig.GcTTL, "gc-ttl", o.serverConfig.GcTTL, "CDC GC safepoint TTL duration, specified in seconds")

	cmd.Flags().StringVar(&o.serverConfig.LogFile, "log-file", o.serverConfig.LogFile, "log file path")
	cmd.Flags().StringVar(&o.serverConfig.LogLevel, "log-level", o.serverConfig.LogLevel, "log level (etc: debug|info|warn|error)")

	cmd.Flags().StringVar(&o.serverConfig.DataDir, "data-dir", o.serverConfig.DataDir, "the path to the directory used to store TiCDC-generated data")

	cmd.Flags().DurationVar((*time.Duration)(&o.serverConfig.OwnerFlushInterval), "owner-flush-interval", time.Duration(o.serverConfig.OwnerFlushInterval), "owner flushes changefeed status interval")
	_ = cmd.Flags().MarkHidden("owner-flush-interval")

	cmd.Flags().DurationVar((*time.Duration)(&o.serverConfig.ProcessorFlushInterval), "processor-flush-interval", time.Duration(o.serverConfig.ProcessorFlushInterval), "processor flushes task status interval")
	_ = cmd.Flags().MarkHidden("processor-flush-interval")

	// sorter related parameters, hidden them since cannot be configured by TiUP easily.
	cmd.Flags().IntVar(&o.serverConfig.Sorter.NumWorkerPoolGoroutine, "sorter-num-workerpool-goroutine", o.serverConfig.Sorter.NumWorkerPoolGoroutine, "sorter workerpool size")
	_ = cmd.Flags().MarkHidden("sorter-num-workerpool-goroutine")

	cmd.Flags().IntVar(&o.serverConfig.Sorter.NumConcurrentWorker, "sorter-num-concurrent-worker", o.serverConfig.Sorter.NumConcurrentWorker, "sorter concurrency level")
	_ = cmd.Flags().MarkHidden("sorter-num-concurrent-worker")

	cmd.Flags().Uint64Var(&o.serverConfig.Sorter.ChunkSizeLimit, "sorter-chunk-size-limit", o.serverConfig.Sorter.ChunkSizeLimit, "size of heaps for sorting")
	_ = cmd.Flags().MarkHidden("sorter-chunk-size-limit")

	// 80 is safe on most systems.
	cmd.Flags().IntVar(&o.serverConfig.Sorter.MaxMemoryPercentage, "sorter-max-memory-percentage", o.serverConfig.Sorter.MaxMemoryPercentage, "system memory usage threshold for forcing in-disk sort")
	_ = cmd.Flags().MarkHidden("sorter-max-memory-percentage")
	// We use 8GB as a safe default before we support local configuration file.
	cmd.Flags().Uint64Var(&o.serverConfig.Sorter.MaxMemoryConsumption, "sorter-max-memory-consumption", o.serverConfig.Sorter.MaxMemoryConsumption, "maximum memory consumption of in-memory sort")
	_ = cmd.Flags().MarkHidden("sorter-max-memory-consumption")

	// sort-dir id deprecate, hidden it.
	cmd.Flags().StringVar(&o.serverConfig.Sorter.SortDir, "sort-dir", o.serverConfig.Sorter.SortDir, "sorter's temporary file directory")
	_ = cmd.Flags().MarkHidden("sort-dir")

	cmd.Flags().StringVar(&o.serverPdAddr, "pd", "http://127.0.0.1:2379", "Set the PD endpoints to use. Use ',' to separate multiple PDs")
	cmd.Flags().StringVar(&o.serverConfigFilePath, "config", "", "Path of the configuration file")

	cmd.Flags().StringVar(&o.caPath, "ca", "", "CA certificate path for TLS connection")
	cmd.Flags().StringVar(&o.certPath, "cert", "", "Certificate path for TLS connection")
	cmd.Flags().StringVar(&o.keyPath, "key", "", "Private key path for TLS connection")
	cmd.Flags().StringVar(&o.allowedCertCN, "cert-allowed-cn", "", "Verify caller's identity (cert Common Name). Use ',' to separate multiple CN")
}

// run runs the server cmd.
func (o *options) run(cmd *cobra.Command) error {
	cancel := util.InitCmd(cmd, &logutil.Config{
		File:                 o.serverConfig.LogFile,
		Level:                o.serverConfig.LogLevel,
		FileMaxSize:          o.serverConfig.Log.File.MaxSize,
		FileMaxDays:          o.serverConfig.Log.File.MaxDays,
		FileMaxBackups:       o.serverConfig.Log.File.MaxBackups,
		ZapInternalErrOutput: o.serverConfig.Log.InternalErrOutput,
	})
	defer cancel()

	tz, err := ticdcutil.GetTimezone(o.serverConfig.TZ)
	if err != nil {
		return errors.Annotate(err, "can not load timezone, Please specify the time zone through environment variable `TZ` or command line parameters `--tz`")
	}

	config.StoreGlobalServerConfig(o.serverConfig)
	ctx := contextutil.PutTimezoneInCtx(cmdcontext.GetDefaultContext(), tz)
	ctx = contextutil.PutCaptureAddrInCtx(ctx, o.serverConfig.AdvertiseAddr)

	version.LogVersionInfo()
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
	cdc.RecordGoRuntimeSettings()
	server, err := cdc.NewServer(strings.Split(o.serverPdAddr, ","))
	if err != nil {
		return errors.Annotate(err, "new server")
	}
	err = server.Run(ctx)
	if err != nil && errors.Cause(err) != context.Canceled {
		log.Error("run server", zap.String("error", errors.ErrorStack(err)))
		return errors.Annotate(err, "run server")
	}
	server.Close()
	unified.CleanUp()
	log.Info("cdc server exits successfully")

	return nil
}

// complete adapts from the command line args and config file to the data required.
func (o *options) complete(cmd *cobra.Command) error {
	o.serverConfig.Security = o.getCredential()

	cfg := config.GetDefaultServerConfig()

	if len(o.serverConfigFilePath) > 0 {
		// strict decode config file, but ignore debug item
		if err := util.StrictDecodeFile(o.serverConfigFilePath, "TiCDC server", cfg, config.DebugConfigurationItem); err != nil {
			return err
		}

		// User specified sort-dir should not take effect, it's always `/tmp/sorter`
		// if user try to set sort-dir by config file, warn it.
		if cfg.Sorter.SortDir != config.DefaultSortDir {
			cmd.Printf(color.HiYellowString("[WARN] --sort-dir is deprecated in server settings. " +
				"sort-dir will be set to `{data-dir}/tmp/sorter`. The sort-dir here will be no-op\n"))

			cfg.Sorter.SortDir = config.DefaultSortDir
		}
	}

	cmd.Flags().Visit(func(flag *pflag.Flag) {
		switch flag.Name {
		case "addr":
			cfg.Addr = o.serverConfig.Addr
		case "advertise-addr":
			cfg.AdvertiseAddr = o.serverConfig.AdvertiseAddr
		case "tz":
			cfg.TZ = o.serverConfig.TZ
		case "gc-ttl":
			cfg.GcTTL = o.serverConfig.GcTTL
		case "log-file":
			cfg.LogFile = o.serverConfig.LogFile
		case "log-level":
			cfg.LogLevel = o.serverConfig.LogLevel
		case "data-dir":
			cfg.DataDir = o.serverConfig.DataDir
		case "owner-flush-interval":
			cfg.OwnerFlushInterval = o.serverConfig.OwnerFlushInterval
		case "processor-flush-interval":
			cfg.ProcessorFlushInterval = o.serverConfig.ProcessorFlushInterval
		case "sorter-num-workerpool-goroutine":
			cfg.Sorter.NumWorkerPoolGoroutine = o.serverConfig.Sorter.NumWorkerPoolGoroutine
		case "sorter-num-concurrent-worker":
			cfg.Sorter.NumConcurrentWorker = o.serverConfig.Sorter.NumConcurrentWorker
		case "sorter-chunk-size-limit":
			cfg.Sorter.ChunkSizeLimit = o.serverConfig.Sorter.ChunkSizeLimit
		case "sorter-max-memory-percentage":
			cfg.Sorter.MaxMemoryPercentage = o.serverConfig.Sorter.MaxMemoryPercentage
		case "sorter-max-memory-consumption":
			cfg.Sorter.MaxMemoryConsumption = o.serverConfig.Sorter.MaxMemoryConsumption
		case "ca":
			cfg.Security.CAPath = o.serverConfig.Security.CAPath
		case "cert":
			cfg.Security.CertPath = o.serverConfig.Security.CertPath
		case "key":
			cfg.Security.KeyPath = o.serverConfig.Security.KeyPath
		case "cert-allowed-cn":
			cfg.Security.CertAllowedCN = o.serverConfig.Security.CertAllowedCN
		case "sort-dir":
			// user specified sorter dir should not take effect, it's always `/tmp/sorter`
			// if user try to set sort-dir by flag, warn it.
			if o.serverConfig.Sorter.SortDir != config.DefaultSortDir {
				cmd.Printf(color.HiYellowString("[WARN] --sort-dir is deprecated in server settings. " +
					"sort-dir will be set to `{data-dir}/tmp/sorter`. The sort-dir here will be no-op\n"))
			}
			cfg.Sorter.SortDir = config.DefaultSortDir
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
		cmd.Printf(color.HiYellowString("[WARN] TiCDC server data-dir is not set. " +
			"Please use `cdc server --data-dir` to start the cdc server if possible.\n"))
	}

	o.serverConfig = cfg

	return nil
}

// validate checks that the provided attach options are specified.
func (o *options) validate() error {
	if len(o.serverPdAddr) == 0 {
		return cerror.ErrInvalidServerOption.GenWithStack("empty PD address")
	}
	for _, ep := range strings.Split(o.serverPdAddr, ",") {
		// NOTICE: The configuration used here is the one that has been completed,
		// as it may be configured by the configuration file.
		if err := util.VerifyPdEndpoint(ep, o.serverConfig.Security.IsTLSEnabled()); err != nil {
			return cerror.ErrInvalidServerOption.Wrap(err).GenWithStackByCause()
		}
	}
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

// NewCmdServer creates the `server` command.
func NewCmdServer() *cobra.Command {
	o := newOptions()

	command := &cobra.Command{
		Use:   "server",
		Short: "Start a TiCDC capture server",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
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
