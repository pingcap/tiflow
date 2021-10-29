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
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/kafka"
	"github.com/pingcap/ticdc/pkg/kafka/Consumer"
	"github.com/pingcap/ticdc/pkg/logutil"
	"github.com/pingcap/ticdc/pkg/security"
	"go.uber.org/zap"
)

var (
	upstreamURIStr   string
	downstreamURIStr string

	logPath       string
	logLevel      string
	timezone      string
	ca, cert, key string

	protocol string

	changefeedID = "kafka-consumer"
)

func init() {
	flag.StringVar(&upstreamURIStr, "upstream-uri", "", "Kafka uri")
	flag.StringVar(&downstreamURIStr, "downstream-uri", "", "downstream sink uri")
	flag.StringVar(&logPath, "log-file", "cdc_kafka_consumer.log", "log file path")
	flag.StringVar(&logLevel, "log-level", "info", "log file path")
	flag.StringVar(&timezone, "tz", "System", "Specify time zone of Kafka consumer")
	flag.StringVar(&ca, "ca", "", "CA certificate path for Kafka SSL connection")
	flag.StringVar(&cert, "cert", "", "Certificate path for Kafka SSL connection")
	flag.StringVar(&key, "key", "", "Private key path for Kafka SSL connection")
	flag.StringVar(&protocol, "protocol", "", "Message encoding protocol")
	flag.Parse()

	err := logutil.InitLogger(&logutil.Config{
		Level: logLevel,
		File:  logPath,
	})
	if err != nil {
		log.Fatal("init logger failed", zap.Error(err))
	}
}

func main() {
	log.Info("Starting a new TiCDC open protocol consumer")

	config := Consumer.NewConfig()
	if err := config.Initialize(upstreamURIStr); err != nil {
		log.Panic("initialize consumer config failed", zap.Error(err))
	}

	config.WithTimezone()
	config.WithDownstream()

	/**
	 * Construct a new Sarama configuration.
	 * The Kafka cluster version has to be defined before the consumer/producer is initialized.
	 */
	saramaConfig, err := Consumer.NewSaramaConfig(config.Version, &security.Credential{
		CAPath:   ca,
		CertPath: cert,
		KeyPath:  key,
	})
	if err != nil {
		log.Panic("creating saramaConfig failed", zap.Error(err))
	}

	admin, err := kafka.NewClusterAdmin(config.BrokerEndpoints, saramaConfig)
	if err != nil {
		log.Panic("create cluster admin failed", zap.Error(err))
	}
	defer admin.Close()

	if err := admin.WaitTopicCreated(config.Topic); err != nil {
		log.Panic("wait topic created failed", zap.Error(err))
	}

	count, err := admin.GetPartitionCount(config.Topic)
	if err != nil {
		log.Panic("get partition count failed", zap.Error(err))
	}
	config.PartitionCount = count

	/**
	 * Setup a new Sarama consumer group
	 */

	ctx := context.Background()
	consumer, err := Consumer.New(ctx, config)
	if err != nil {
		log.Panic("creating consumer failed", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(ctx)
	client, err := sarama.NewConsumerGroup(config.BrokerEndpoints, config.GroupID, saramaConfig)
	if err != nil {
		log.Fatal("Error creating consumer group client", zap.Error(err))
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(ctx, strings.Split(config.Topic, ","), consumer); err != nil {
				log.Fatal("Error from consumer: %v", zap.Error(err))
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.Ready = make(chan bool)
		}
	}()

	go func() {
		if err := consumer.Run(ctx); err != nil {
			log.Fatal("Error running consumer: %v", zap.Error(err))
		}
	}()

	<-consumer.Ready // Await till the consumer has been set up
	log.Info("TiCDC open protocol consumer up and running!...")

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
	if err = client.Close(); err != nil {
		log.Fatal("Error closing client", zap.Error(err))
	}
}
