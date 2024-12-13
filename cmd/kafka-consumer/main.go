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

package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/pingcap/tiflow/pkg/version"
	"go.uber.org/zap"
)

func main() {
	var (
		upstreamURIStr string
		configFile     string
	)
	groupID := fmt.Sprintf("ticdc_kafka_consumer_%s", uuid.New().String())
	consumerOption := newOption()
	flag.StringVar(&configFile, "config", "", "config file for changefeed")
	flag.StringVar(&upstreamURIStr, "upstream-uri", "", "Kafka uri")
	flag.StringVar(&consumerOption.downstreamURI, "downstream-uri", "", "downstream sink uri")
	flag.StringVar(&consumerOption.schemaRegistryURI, "schema-registry-uri", "", "schema registry uri")
	flag.StringVar(&consumerOption.upstreamTiDBDSN, "upstream-tidb-dsn", "", "upstream TiDB DSN")
	flag.StringVar(&consumerOption.groupID, "consumer-group-id", groupID, "consumer group id")
	flag.StringVar(&consumerOption.logPath, "log-file", "cdc_kafka_consumer.log", "log file path")
	flag.StringVar(&consumerOption.logLevel, "log-level", "info", "log file path")
	flag.StringVar(&consumerOption.timezone, "tz", "System", "Specify time zone of Kafka consumer")
	flag.StringVar(&consumerOption.ca, "ca", "", "CA certificate path for Kafka SSL connection")
	flag.StringVar(&consumerOption.cert, "cert", "", "Certificate path for Kafka SSL connection")
	flag.StringVar(&consumerOption.key, "key", "", "Private key path for Kafka SSL connection")
	flag.BoolVar(&consumerOption.enableProfiling, "enable-profiling", false, "enable pprof profiling")
	flag.Parse()

	err := logutil.InitLogger(&logutil.Config{
		Level: consumerOption.logLevel,
		File:  consumerOption.logPath,
	})
	if err != nil {
		log.Panic("init logger failed", zap.Error(err))
	}
	version.LogVersionInfo("kafka consumer")

	upstreamURI, err := url.Parse(upstreamURIStr)
	if err != nil {
		log.Panic("invalid upstream-uri", zap.Error(err))
	}
	scheme := strings.ToLower(upstreamURI.Scheme)
	if scheme != "kafka" {
		log.Panic("invalid upstream-uri scheme, the scheme of upstream-uri must be `kafka`",
			zap.String("upstreamURI", upstreamURIStr))
	}

	err = consumerOption.Adjust(upstreamURI, configFile)
	if err != nil {
		log.Panic("adjust consumer option failed", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	consumer := newConsumer(ctx, consumerOption)
	var wg sync.WaitGroup
	if consumerOption.enableProfiling {
		log.Info("profiling is enabled")
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err = http.ListenAndServe("127.0.0.1:6060", nil); err != nil {
				log.Panic("cannot start the pprof", zap.Error(err))
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		consumer.Consume(ctx)
	}()

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Info("terminating: context cancelled")
	case <-sigterm:
		log.Info("terminating: via signal")
	}
	cancel()
	wg.Wait()
}
