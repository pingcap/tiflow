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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/logutil"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/version"
	ticonfig "github.com/pingcap/tidb/config"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	serverPdAddr  string
	address       string
	advertiseAddr string
	timezone      string
	gcTTL         int64
	logFile       string
	logLevel      string
	// variables for unified sorter
	numConcurrentWorker  int
	chunkSizeLimit       uint64
	maxMemoryPressure    int
	maxMemoryConsumption uint64

	ownerFlushInterval     time.Duration
	processorFlushInterval time.Duration

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

	serverCmd.Flags().StringVar(&serverPdAddr, "pd", "http://127.0.0.1:2379", "Set the PD endpoints to use. Use ',' to separate multiple PDs")
	serverCmd.Flags().StringVar(&address, "addr", "127.0.0.1:8300", "Set the listening address")
	serverCmd.Flags().StringVar(&advertiseAddr, "advertise-addr", "", "Set the advertise listening address for client communication")
	serverCmd.Flags().StringVar(&timezone, "tz", "System", "Specify time zone of TiCDC cluster")
	serverCmd.Flags().Int64Var(&gcTTL, "gc-ttl", cdc.DefaultCDCGCSafePointTTL, "CDC GC safepoint TTL duration, specified in seconds")
	serverCmd.Flags().StringVar(&logFile, "log-file", "", "log file path")
	serverCmd.Flags().StringVar(&logLevel, "log-level", "info", "log level (etc: debug|info|warn|error)")
	serverCmd.Flags().DurationVar(&ownerFlushInterval, "owner-flush-interval", time.Millisecond*200, "owner flushes changefeed status interval")
	serverCmd.Flags().DurationVar(&processorFlushInterval, "processor-flush-interval", time.Millisecond*100, "processor flushes task status interval")

	serverCmd.Flags().IntVar(&numConcurrentWorker, "sorter-num-concurrent-worker", 8, "sorter concurrency level")
	serverCmd.Flags().Uint64Var(&chunkSizeLimit, "sorter-chunk-size-limit", 1024*1024*1024, "size of heaps for sorting")
	serverCmd.Flags().IntVar(&maxMemoryPressure, "sorter-max-memory-percentage", 90, "system memory usage threshold for forcing in-disk sort")
	serverCmd.Flags().Uint64Var(&maxMemoryConsumption, "sorter-max-memory-consumption", 16*1024*1024*1024, "maximum memory consumption of in-memory sort")

	addSecurityFlags(serverCmd.Flags(), true /* isServer */)
}

func runEServer(cmd *cobra.Command, args []string) error {
	cancel := initCmd(cmd, &logutil.Config{
		File:  logFile,
		Level: logLevel,
	})
	defer cancel()
	tz, err := util.GetTimezone(timezone)
	if err != nil {
		return errors.Annotate(err, "can not load timezone, Please specify the time zone through environment variable `TZ` or command line parameters `--tz`")
	}

	config.SetSorterConfig(&config.SorterConfig{
		NumConcurrentWorker:  numConcurrentWorker,
		ChunkSizeLimit:       chunkSizeLimit,
		MaxMemoryPressure:    maxMemoryPressure,
		MaxMemoryConsumption: maxMemoryConsumption,
	})

	version.LogVersionInfo()
	opts := []cdc.ServerOption{
		cdc.PDEndpoints(serverPdAddr),
		cdc.Address(address),
		cdc.AdvertiseAddress(advertiseAddr),
		cdc.GCTTL(gcTTL),
		cdc.Timezone(tz),
		cdc.Credential(getCredential()),
		cdc.OwnerFlushInterval(ownerFlushInterval),
		cdc.ProcessorFlushInterval(processorFlushInterval),
	}
	server, err := cdc.NewServer(opts...)
	if err != nil {
		return errors.Annotate(err, "new server")
	}
	err = server.Run(defaultContext)
	if err != nil && errors.Cause(err) != context.Canceled {
		log.Error("run server", zap.String("error", errors.ErrorStack(err)))
		return errors.Annotate(err, "run server")
	}
	server.Close()
	log.Info("cdc server exits successfully")

	return nil
}
