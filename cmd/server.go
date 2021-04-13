// Copyright 2020 PingCAP, Inc.
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

package cmd

import (
	"context"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc"
	"github.com/pingcap/ticdc/cdc/puller/sorter"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/logutil"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/version"
	ticonfig "github.com/pingcap/tidb/config"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

var (
	serverPdAddr         string
	serverConfigFilePath string

	serverConfig = config.GetDefaultServerConfig()

	serverCmd = &cobra.Command{
		Use:   "server",
		Short: "Start a TiCDC capture server",
		RunE:  runEServer,
	}
)

func patchTiDBConf() {
	ticonfig.UpdateGlobal(func(conf *ticonfig.Config) {
		// Disable kv client batch send loop introduced by tidb library, which is not used in TiCDC server
		conf.TiKVClient.MaxBatchSize = 0
	})
}

func init() {
	patchTiDBConf()
	rootCmd.AddCommand(serverCmd)
	initServerCmd(serverCmd)
}

func initServerCmd(cmd *cobra.Command) {
	defaultServerConfig := config.GetDefaultServerConfig()
	cmd.Flags().StringVar(&serverPdAddr, "pd", "http://127.0.0.1:2379", "Set the PD endpoints to use. Use ',' to separate multiple PDs")
	cmd.Flags().StringVar(&serverConfig.Addr, "addr", defaultServerConfig.Addr, "Set the listening address")
	cmd.Flags().StringVar(&serverConfig.AdvertiseAddr, "advertise-addr", defaultServerConfig.AdvertiseAddr, "Set the advertise listening address for client communication")
	cmd.Flags().StringVar(&serverConfig.TZ, "tz", defaultServerConfig.TZ, "Specify time zone of TiCDC cluster")
	cmd.Flags().Int64Var(&serverConfig.GcTTL, "gc-ttl", defaultServerConfig.GcTTL, "CDC GC safepoint TTL duration, specified in seconds")
	cmd.Flags().StringVar(&serverConfig.LogFile, "log-file", defaultServerConfig.LogFile, "log file path")
	cmd.Flags().StringVar(&serverConfig.LogLevel, "log-level", defaultServerConfig.LogLevel, "log level (etc: debug|info|warn|error)")
	cmd.Flags().DurationVar((*time.Duration)(&serverConfig.OwnerFlushInterval), "owner-flush-interval", time.Duration(defaultServerConfig.OwnerFlushInterval), "owner flushes changefeed status interval")
	cmd.Flags().DurationVar((*time.Duration)(&serverConfig.ProcessorFlushInterval), "processor-flush-interval", time.Duration(defaultServerConfig.ProcessorFlushInterval), "processor flushes task status interval")

	cmd.Flags().IntVar(&serverConfig.Sorter.NumWorkerPoolGoroutine, "sorter-num-workerpool-goroutine", defaultServerConfig.Sorter.NumWorkerPoolGoroutine, "sorter workerpool size")
	cmd.Flags().IntVar(&serverConfig.Sorter.NumConcurrentWorker, "sorter-num-concurrent-worker", defaultServerConfig.Sorter.NumConcurrentWorker, "sorter concurrency level")
	cmd.Flags().Uint64Var(&serverConfig.Sorter.ChunkSizeLimit, "sorter-chunk-size-limit", defaultServerConfig.Sorter.ChunkSizeLimit, "size of heaps for sorting")
	// 80 is safe on most systems.
	cmd.Flags().IntVar(&serverConfig.Sorter.MaxMemoryPressure, "sorter-max-memory-percentage", defaultServerConfig.Sorter.MaxMemoryPressure, "system memory usage threshold for forcing in-disk sort")
	// We use 8GB as a safe default before we support local configuration file.
	cmd.Flags().Uint64Var(&serverConfig.Sorter.MaxMemoryConsumption, "sorter-max-memory-consumption", defaultServerConfig.Sorter.MaxMemoryConsumption, "maximum memory consumption of in-memory sort")
	cmd.Flags().StringVar(&serverConfig.Sorter.SortDir, "sort-dir", defaultServerConfig.Sorter.SortDir, "sorter's temporary file directory")

	addSecurityFlags(cmd.Flags(), true /* isServer */)

	cmd.Flags().StringVar(&serverConfigFilePath, "config", "", "Path of the configuration file")
}

func runEServer(cmd *cobra.Command, args []string) error {
	conf, err := loadAndVerifyServerConfig(cmd)
	if err != nil {
		return errors.Trace(err)
	}

	cancel := initCmd(cmd, &logutil.Config{
		File:  conf.LogFile,
		Level: conf.LogLevel,
	})
	defer cancel()
	tz, err := util.GetTimezone(conf.TZ)
	if err != nil {
		return errors.Annotate(err, "can not load timezone, Please specify the time zone through environment variable `TZ` or command line parameters `--tz`")
	}
	config.StoreGlobalServerConfig(conf)
	ctx := util.PutTimezoneInCtx(defaultContext, tz)
	ctx = util.PutCaptureAddrInCtx(ctx, conf.AdvertiseAddr)

	version.LogVersionInfo()

	logHTTPProxies()
	server, err := cdc.NewServer(strings.Split(serverPdAddr, ","))
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

func loadAndVerifyServerConfig(cmd *cobra.Command) (*config.ServerConfig, error) {
	serverConfig.Security = getCredential()

	conf := config.GetDefaultServerConfig()
	if len(serverConfigFilePath) > 0 {
		if err := strictDecodeFile(serverConfigFilePath, "TiCDC server", conf); err != nil {
			return nil, err
		}
	}
	cmd.Flags().Visit(func(flag *pflag.Flag) {
		switch flag.Name {
		case "addr":
			conf.Addr = serverConfig.Addr
		case "advertise-addr":
			conf.AdvertiseAddr = serverConfig.AdvertiseAddr
		case "tz":
			conf.TZ = serverConfig.TZ
		case "gc-ttl":
			conf.GcTTL = serverConfig.GcTTL
		case "log-file":
			conf.LogFile = serverConfig.LogFile
		case "log-level":
			conf.LogLevel = serverConfig.LogLevel
		case "owner-flush-interval":
			conf.OwnerFlushInterval = serverConfig.OwnerFlushInterval
		case "processor-flush-interval":
			conf.ProcessorFlushInterval = serverConfig.ProcessorFlushInterval
		case "sorter-num-workerpool-goroutine":
			conf.Sorter.NumWorkerPoolGoroutine = serverConfig.Sorter.NumWorkerPoolGoroutine
		case "sorter-num-concurrent-worker":
			conf.Sorter.NumConcurrentWorker = serverConfig.Sorter.NumConcurrentWorker
		case "sorter-chunk-size-limit":
			conf.Sorter.ChunkSizeLimit = serverConfig.Sorter.ChunkSizeLimit
		case "sorter-max-memory-percentage":
			conf.Sorter.MaxMemoryPressure = serverConfig.Sorter.MaxMemoryPressure
		case "sorter-max-memory-consumption":
			conf.Sorter.MaxMemoryConsumption = serverConfig.Sorter.MaxMemoryConsumption
		case "ca":
			conf.Security.CAPath = serverConfig.Security.CAPath
		case "cert":
			conf.Security.CertPath = serverConfig.Security.CertPath
		case "key":
			conf.Security.KeyPath = serverConfig.Security.KeyPath
		case "cert-allowed-cn":
			conf.Security.CertAllowedCN = serverConfig.Security.CertAllowedCN
		case "sort-dir":
			conf.Sorter.SortDir = serverConfig.Sorter.SortDir
		case "pd", "config":
			// do nothing
		default:
			log.Panic("unknown flag, please report a bug", zap.String("flagName", flag.Name))
		}
	})
	if err := conf.ValidateAndAdjust(); err != nil {
		return nil, errors.Trace(err)
	}
	if len(serverPdAddr) == 0 {
		return nil, cerror.ErrInvalidServerOption.GenWithStack("empty PD address")
	}
	for _, ep := range strings.Split(serverPdAddr, ",") {
		if conf.Security.IsTLSEnabled() {
			if strings.Index(ep, "http://") == 0 {
				return nil, cerror.ErrInvalidServerOption.GenWithStack("PD endpoint scheme should be https")
			}
		} else if strings.Index(ep, "http://") != 0 {
			return nil, cerror.ErrInvalidServerOption.GenWithStack("PD endpoint scheme should be http")
		}
	}

	return conf, nil
}
