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
	serverPdAddr         string
	serverConfigFilePath string
	caPath               string
	certPath             string
	keyPath              string
	allowedCertCN        string

	serverConfig *config.ServerConfig
}

// newOptions creates new options for the `server` command.
func newOptions() *options {
	return &options{}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *options) addFlags(cmd *cobra.Command) {
	defaultServerConfig := config.GetDefaultServerConfig()
	cmd.Flags().StringVar(&o.serverPdAddr, "pd", "http://127.0.0.1:2379", "Set the PD endpoints to use. Use ',' to separate multiple PDs")
	cmd.Flags().StringVar(&o.serverConfig.Addr, "addr", defaultServerConfig.Addr, "Set the listening address")
	cmd.Flags().StringVar(&o.serverConfig.AdvertiseAddr, "advertise-addr", defaultServerConfig.AdvertiseAddr, "Set the advertise listening address for client communication")
	cmd.Flags().StringVar(&o.serverConfig.TZ, "tz", defaultServerConfig.TZ, "Specify time zone of TiCDC cluster")
	cmd.Flags().Int64Var(&o.serverConfig.GcTTL, "gc-ttl", defaultServerConfig.GcTTL, "CDC GC safepoint TTL duration, specified in seconds")
	cmd.Flags().StringVar(&o.serverConfig.LogFile, "log-file", defaultServerConfig.LogFile, "log file path")
	cmd.Flags().StringVar(&o.serverConfig.LogLevel, "log-level", defaultServerConfig.LogLevel, "log level (etc: debug|info|warn|error)")
	cmd.Flags().StringVar(&o.serverConfig.DataDir, "data-dir", defaultServerConfig.DataDir, "the path to the directory used to store TiCDC-generated data")
	cmd.Flags().DurationVar((*time.Duration)(&o.serverConfig.OwnerFlushInterval), "owner-flush-interval", time.Duration(defaultServerConfig.OwnerFlushInterval), "owner flushes changefeed status interval")
	cmd.Flags().DurationVar((*time.Duration)(&o.serverConfig.ProcessorFlushInterval), "processor-flush-interval", time.Duration(defaultServerConfig.ProcessorFlushInterval), "processor flushes task status interval")

	cmd.Flags().IntVar(&o.serverConfig.Sorter.NumWorkerPoolGoroutine, "sorter-num-workerpool-goroutine", defaultServerConfig.Sorter.NumWorkerPoolGoroutine, "sorter workerpool size")
	cmd.Flags().IntVar(&o.serverConfig.Sorter.NumConcurrentWorker, "sorter-num-concurrent-worker", defaultServerConfig.Sorter.NumConcurrentWorker, "sorter concurrency level")
	cmd.Flags().Uint64Var(&o.serverConfig.Sorter.ChunkSizeLimit, "sorter-chunk-size-limit", defaultServerConfig.Sorter.ChunkSizeLimit, "size of heaps for sorting")
	// 80 is safe on most systems.
	cmd.Flags().IntVar(&o.serverConfig.Sorter.MaxMemoryPressure, "sorter-max-memory-percentage", defaultServerConfig.Sorter.MaxMemoryPressure, "system memory usage threshold for forcing in-disk sort")
	// We use 8GB as a safe default before we support local configuration file.
	cmd.Flags().Uint64Var(&o.serverConfig.Sorter.MaxMemoryConsumption, "sorter-max-memory-consumption", defaultServerConfig.Sorter.MaxMemoryConsumption, "maximum memory consumption of in-memory sort")
	cmd.Flags().StringVar(&o.serverConfig.Sorter.SortDir, "sort-dir", defaultServerConfig.Sorter.SortDir, "sorter's temporary file directory")

	cmd.Flags().StringVar(&o.caPath, "ca", "", "CA certificate path for TLS connection")
	cmd.Flags().StringVar(&o.certPath, "cert", "", "Certificate path for TLS connection")
	cmd.Flags().StringVar(&o.keyPath, "key", "", "Private key path for TLS connection")
	cmd.Flags().StringVar(&o.allowedCertCN, "cert-allowed-cn", "", "Verify caller's identity (cert Common Name). Use ',' to separate multiple CN")

	cmd.Flags().StringVar(&o.serverConfigFilePath, "config", "", "Path of the configuration file")
	_ = cmd.Flags().MarkHidden("sort-dir") //nolint:errcheck
}

func (o *options) run(cmd *cobra.Command) error {
	conf, err := o.loadAndVerifyServerConfig(cmd)
	if err != nil {
		return errors.Trace(err)
	}

	cancel := util.InitCmd(cmd, &logutil.Config{
		File:           conf.LogFile,
		Level:          conf.LogLevel,
		FileMaxSize:    conf.Log.File.MaxSize,
		FileMaxDays:    conf.Log.File.MaxDays,
		FileMaxBackups: conf.Log.File.MaxBackups,
	})
	defer cancel()
	tz, err := ticdcutil.GetTimezone(conf.TZ)
	if err != nil {
		return errors.Annotate(err, "can not load timezone, Please specify the time zone through environment variable `TZ` or command line parameters `--tz`")
	}
	config.StoreGlobalServerConfig(conf)
	ctx := ticdcutil.PutTimezoneInCtx(cmdcontext.GetDefaultContext(), tz)
	ctx = ticdcutil.PutCaptureAddrInCtx(ctx, conf.AdvertiseAddr)

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

func (o *options) loadAndVerifyServerConfig(cmd *cobra.Command) (*config.ServerConfig, error) {
	o.serverConfig.Security = o.getCredential()

	conf := config.GetDefaultServerConfig()
	if len(o.serverConfigFilePath) > 0 {
		if err := util.StrictDecodeFile(o.serverConfigFilePath, "TiCDC server", conf); err != nil {
			return nil, err
		}
	}
	cmd.Flags().Visit(func(flag *pflag.Flag) {
		switch flag.Name {
		case "addr":
			conf.Addr = o.serverConfig.Addr
		case "advertise-addr":
			conf.AdvertiseAddr = o.serverConfig.AdvertiseAddr
		case "tz":
			conf.TZ = o.serverConfig.TZ
		case "gc-ttl":
			conf.GcTTL = o.serverConfig.GcTTL
		case "log-file":
			conf.LogFile = o.serverConfig.LogFile
		case "log-level":
			conf.LogLevel = o.serverConfig.LogLevel
		case "data-dir":
			conf.DataDir = o.serverConfig.DataDir
		case "owner-flush-interval":
			conf.OwnerFlushInterval = o.serverConfig.OwnerFlushInterval
		case "processor-flush-interval":
			conf.ProcessorFlushInterval = o.serverConfig.ProcessorFlushInterval
		case "sorter-num-workerpool-goroutine":
			conf.Sorter.NumWorkerPoolGoroutine = o.serverConfig.Sorter.NumWorkerPoolGoroutine
		case "sorter-num-concurrent-worker":
			conf.Sorter.NumConcurrentWorker = o.serverConfig.Sorter.NumConcurrentWorker
		case "sorter-chunk-size-limit":
			conf.Sorter.ChunkSizeLimit = o.serverConfig.Sorter.ChunkSizeLimit
		case "sorter-max-memory-percentage":
			conf.Sorter.MaxMemoryPressure = o.serverConfig.Sorter.MaxMemoryPressure
		case "sorter-max-memory-consumption":
			conf.Sorter.MaxMemoryConsumption = o.serverConfig.Sorter.MaxMemoryConsumption
		case "ca":
			conf.Security.CAPath = o.serverConfig.Security.CAPath
		case "cert":
			conf.Security.CertPath = o.serverConfig.Security.CertPath
		case "key":
			conf.Security.KeyPath = o.serverConfig.Security.KeyPath
		case "cert-allowed-cn":
			conf.Security.CertAllowedCN = o.serverConfig.Security.CertAllowedCN
		case "sort-dir":
			// user specified sorter dir should not take effect
			if o.serverConfig.Sorter.SortDir != config.DefaultSortDir {
				cmd.Printf(color.HiYellowString("[WARN] --sort-dir is deprecated in server settings. " +
					"sort-dir will be set to `{data-dir}/tmp/sorter`. The sort-dir here will be no-op\n"))
			}
			conf.Sorter.SortDir = config.DefaultSortDir
		case "pd", "config":
			// do nothing
		default:
			log.Panic("unknown flag, please report a bug", zap.String("flagName", flag.Name))
		}
	})
	if err := conf.ValidateAndAdjust(); err != nil {
		return nil, errors.Trace(err)
	}
	if len(o.serverPdAddr) == 0 {
		return nil, cerror.ErrInvalidServerOption.GenWithStack("empty PD address")
	}
	for _, ep := range strings.Split(o.serverPdAddr, ",") {
		if err := util.VerifyPdEndpoint(ep, conf.Security.IsTLSEnabled()); err != nil {
			return nil, cerror.ErrInvalidServerOption.Wrap(err).GenWithStackByCause()
		}
	}

	if conf.DataDir == "" {
		cmd.Printf(color.HiYellowString("[WARN] TiCDC server data-dir is not set. " +
			"Please use `cdc server --data-dir` to start the cdc server if possible.\n"))
	}

	return conf, nil
}

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
