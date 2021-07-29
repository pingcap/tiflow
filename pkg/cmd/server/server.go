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
	"github.com/pingcap/ticdc/cdc"
	"github.com/pingcap/ticdc/cdc/puller/sorter"
	cmdcontext "github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/logutil"
	"github.com/pingcap/ticdc/pkg/security"
	ticdcutil "github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/version"
	ticonfig "github.com/pingcap/tidb/config"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

// options defines flags for the `server` command.
type options struct {
	serverConfigOptions  *config.ServerConfig
	serverPdAddr         string
	serverConfigFilePath string

	caPath        string
	certPath      string
	keyPath       string
	allowedCertCN string

	finalCfg *config.ServerConfig
}

// newOptions creates new options for the `server` command.
func newOptions() *options {
	return &options{
		serverConfigOptions: config.GetDefaultServerConfig(),
		finalCfg:            config.GetDefaultServerConfig(),
	}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *options) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&o.serverConfigOptions.Addr, "addr", o.finalCfg.Addr, "Set the listening address")
	cmd.Flags().StringVar(&o.serverConfigOptions.AdvertiseAddr, "advertise-addr", o.finalCfg.AdvertiseAddr, "Set the advertise listening address for client communication")
	cmd.Flags().StringVar(&o.serverConfigOptions.TZ, "tz", o.finalCfg.TZ, "Specify time zone of TiCDC cluster")
	cmd.Flags().Int64Var(&o.serverConfigOptions.GcTTL, "gc-ttl", o.finalCfg.GcTTL, "CDC GC safepoint TTL duration, specified in seconds")
	cmd.Flags().StringVar(&o.serverConfigOptions.LogFile, "log-file", o.finalCfg.LogFile, "log file path")
	cmd.Flags().StringVar(&o.serverConfigOptions.LogLevel, "log-level", o.finalCfg.LogLevel, "log level (etc: debug|info|warn|error)")
	cmd.Flags().StringVar(&o.serverConfigOptions.DataDir, "data-dir", o.finalCfg.DataDir, "the path to the directory used to store TiCDC-generated data")
	cmd.Flags().DurationVar((*time.Duration)(&o.serverConfigOptions.OwnerFlushInterval), "owner-flush-interval", time.Duration(o.finalCfg.OwnerFlushInterval), "owner flushes changefeed status interval")
	cmd.Flags().DurationVar((*time.Duration)(&o.serverConfigOptions.ProcessorFlushInterval), "processor-flush-interval", time.Duration(o.finalCfg.ProcessorFlushInterval), "processor flushes task status interval")
	cmd.Flags().IntVar(&o.serverConfigOptions.Sorter.NumWorkerPoolGoroutine, "sorter-num-workerpool-goroutine", o.finalCfg.Sorter.NumWorkerPoolGoroutine, "sorter workerpool size")
	cmd.Flags().IntVar(&o.serverConfigOptions.Sorter.NumConcurrentWorker, "sorter-num-concurrent-worker", o.finalCfg.Sorter.NumConcurrentWorker, "sorter concurrency level")
	cmd.Flags().Uint64Var(&o.serverConfigOptions.Sorter.ChunkSizeLimit, "sorter-chunk-size-limit", o.finalCfg.Sorter.ChunkSizeLimit, "size of heaps for sorting")
	// 80 is safe on most systems.
	cmd.Flags().IntVar(&o.serverConfigOptions.Sorter.MaxMemoryPressure, "sorter-max-memory-percentage", o.finalCfg.Sorter.MaxMemoryPressure, "system memory usage threshold for forcing in-disk sort")
	// We use 8GB as a safe default before we support local configuration file.
	cmd.Flags().Uint64Var(&o.serverConfigOptions.Sorter.MaxMemoryConsumption, "sorter-max-memory-consumption", o.finalCfg.Sorter.MaxMemoryConsumption, "maximum memory consumption of in-memory sort")
	cmd.Flags().StringVar(&o.serverConfigOptions.Sorter.SortDir, "sort-dir", o.finalCfg.Sorter.SortDir, "sorter's temporary file directory")
	cmd.Flags().StringVar(&o.serverPdAddr, "pd", "http://127.0.0.1:2379", "Set the PD endpoints to use. Use ',' to separate multiple PDs")
	cmd.Flags().StringVar(&o.serverConfigFilePath, "config", "", "Path of the configuration file")

	cmd.Flags().StringVar(&o.caPath, "ca", "", "CA certificate path for TLS connection")
	cmd.Flags().StringVar(&o.certPath, "cert", "", "Certificate path for TLS connection")
	cmd.Flags().StringVar(&o.keyPath, "key", "", "Private key path for TLS connection")
	cmd.Flags().StringVar(&o.allowedCertCN, "cert-allowed-cn", "", "Verify caller's identity (cert Common Name). Use ',' to separate multiple CN")
	_ = cmd.Flags().MarkHidden("sort-dir")
}

// run runs the server cmd.
func (o *options) run(cmd *cobra.Command) error {
	cfg := o.finalCfg

	cancel := util.InitCmd(cmd, &logutil.Config{
		File:           cfg.LogFile,
		Level:          cfg.LogLevel,
		FileMaxSize:    cfg.Log.File.MaxSize,
		FileMaxDays:    cfg.Log.File.MaxDays,
		FileMaxBackups: cfg.Log.File.MaxBackups,
	})
	defer cancel()

	tz, err := ticdcutil.GetTimezone(cfg.TZ)
	if err != nil {
		return errors.Annotate(err, "can not load timezone, Please specify the time zone through environment variable `TZ` or command line parameters `--tz`")
	}

	config.StoreGlobalServerConfig(cfg)
	ctx := ticdcutil.PutTimezoneInCtx(cmdcontext.GetDefaultContext(), tz)
	ctx = ticdcutil.PutCaptureAddrInCtx(ctx, cfg.AdvertiseAddr)

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
	sorter.UnifiedSorterCleanUp()
	log.Info("cdc server exits successfully")

	return nil
}

// complete adapts from the command line args and factory to the data required.
func (o *options) complete(cmd *cobra.Command) error {
	o.serverConfigOptions.Security = o.getCredential()

	if len(o.serverConfigFilePath) > 0 {
		if err := util.StrictDecodeFile(o.serverConfigFilePath, "TiCDC server", o.finalCfg); err != nil {
			return err
		}

		// User specified sort-dir should not take effect, it's always `/tmp/sorter`
		// if user try to set sort-dir by config file, warn it.
		if o.finalCfg.Sorter.SortDir != config.DefaultSortDir {
			cmd.Printf(color.HiYellowString("[WARN] --sort-dir is deprecated in server settings. " +
				"sort-dir will be set to `{data-dir}/tmp/sorter`. The sort-dir here will be no-op\n"))

			o.finalCfg.Sorter.SortDir = config.DefaultSortDir
		}
	}

	cmd.Flags().Visit(func(flag *pflag.Flag) {
		switch flag.Name {
		case "addr":
			o.finalCfg.Addr = o.serverConfigOptions.Addr
		case "advertise-addr":
			o.finalCfg.AdvertiseAddr = o.serverConfigOptions.AdvertiseAddr
		case "tz":
			o.finalCfg.TZ = o.serverConfigOptions.TZ
		case "gc-ttl":
			o.finalCfg.GcTTL = o.serverConfigOptions.GcTTL
		case "log-file":
			o.finalCfg.LogFile = o.serverConfigOptions.LogFile
		case "log-level":
			o.finalCfg.LogLevel = o.serverConfigOptions.LogLevel
		case "data-dir":
			o.finalCfg.DataDir = o.serverConfigOptions.DataDir
		case "owner-flush-interval":
			o.finalCfg.OwnerFlushInterval = o.serverConfigOptions.OwnerFlushInterval
		case "processor-flush-interval":
			o.finalCfg.ProcessorFlushInterval = o.serverConfigOptions.ProcessorFlushInterval
		case "sorter-num-workerpool-goroutine":
			o.finalCfg.Sorter.NumWorkerPoolGoroutine = o.serverConfigOptions.Sorter.NumWorkerPoolGoroutine
		case "sorter-num-concurrent-worker":
			o.finalCfg.Sorter.NumConcurrentWorker = o.serverConfigOptions.Sorter.NumConcurrentWorker
		case "sorter-chunk-size-limit":
			o.finalCfg.Sorter.ChunkSizeLimit = o.serverConfigOptions.Sorter.ChunkSizeLimit
		case "sorter-max-memory-percentage":
			o.finalCfg.Sorter.MaxMemoryPressure = o.serverConfigOptions.Sorter.MaxMemoryPressure
		case "sorter-max-memory-consumption":
			o.finalCfg.Sorter.MaxMemoryConsumption = o.serverConfigOptions.Sorter.MaxMemoryConsumption
		case "ca":
			o.finalCfg.Security.CAPath = o.serverConfigOptions.Security.CAPath
		case "cert":
			o.finalCfg.Security.CertPath = o.serverConfigOptions.Security.CertPath
		case "key":
			o.finalCfg.Security.KeyPath = o.serverConfigOptions.Security.KeyPath
		case "cert-allowed-cn":
			o.finalCfg.Security.CertAllowedCN = o.serverConfigOptions.Security.CertAllowedCN
		case "sort-dir":
			// user specified sorter dir should not take effect, it's always `/tmp/sorter`
			// if user try to set sort-dir by flag, warn it.
			if o.serverConfigOptions.Sorter.SortDir != config.DefaultSortDir {
				cmd.Printf(color.HiYellowString("[WARN] --sort-dir is deprecated in server settings. " +
					"sort-dir will be set to `{data-dir}/tmp/sorter`. The sort-dir here will be no-op\n"))
			}
			o.finalCfg.Sorter.SortDir = config.DefaultSortDir
		case "pd", "config":
			// do nothing
		default:
			log.Panic("unknown flag, please report a bug", zap.String("flagName", flag.Name))
		}
	})

	if err := o.finalCfg.ValidateAndAdjust(); err != nil {
		return errors.Trace(err)
	}

	if o.finalCfg.DataDir == "" {
		cmd.Printf(color.HiYellowString("[WARN] TiCDC server data-dir is not set. " +
			"Please use `cdc server --data-dir` to start the cdc server if possible.\n"))
	}

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
		if err := util.VerifyPdEndpoint(ep, o.finalCfg.Security.IsTLSEnabled()); err != nil {
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
		RunE: func(cmd *cobra.Command, args []string) error {
			err := o.complete(cmd)
			if err != nil {
				return err
			}
			err = o.validate()
			if err != nil {
				return err
			}
			return o.run(cmd)
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
