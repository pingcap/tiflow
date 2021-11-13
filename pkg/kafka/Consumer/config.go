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

package Consumer

import (
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/security"
	"go.uber.org/zap"
)

// Config is for Kafka consumer
type Config struct {
	BrokerEndpoints []string

	Topic           string
	PartitionNum    int32
	GroupID         string
	Version         string
	maxMessageBytes int
	maxBatchSize    int

	timezone string

	downstreamStr string
	changefeedID  string

	protocol            string
	EnableTiDBExtension bool
}

// NewConfig return a default `Config`
func NewConfig(upstream, downstream string) (*Config, error) {
	if upstream == "" || downstream == "" {
		return nil, errors.Errorf("upstream-url or downstream-url not found")
	}

	uri, err := url.Parse(upstream)
	if err != nil {
		return nil, errors.Trace(err)
	}
	scheme := strings.ToLower(uri.Scheme)
	if scheme != "kafka" {
		return nil, errors.Errorf("scheme is not kafka, but %v", scheme)
	}

	topic := strings.TrimFunc(uri.Path, func(r rune) bool {
		return r == '/'
	})
	if topic == "" {
		return nil, errors.New("topic should be given")
	}

	result := &Config{
		Topic:           topic,
		downstreamStr:   downstream,
		timezone:        "system",
		Version:         "2.4.0",
		maxMessageBytes: math.MaxInt,
		maxBatchSize:    math.MaxInt,
		GroupID:         fmt.Sprintf("ticdc_kafka_consumer_%s", uuid.New().String()),
		changefeedID:    "kafka-consumer",
	}

	endPoints := strings.Split(uri.Host, ",")
	if len(endPoints) == 0 {
		return nil, errors.New("kafka broker addresses not found")
	}
	result.BrokerEndpoints = endPoints

	params := uri.Query()
	if s := params.Get("version"); s != "" {
		result.Version = s
	}
	if s := params.Get("consumer-group-id"); s != "" {
		result.GroupID = s
	}

	if s := params.Get("partition-num"); s != "" {
		a, err := strconv.Atoi(s)
		if err != nil {
			return nil, errors.Trace(err)
		}
		result.PartitionNum = int32(a)
	}

	if s := params.Get("max-message-bytes"); s != "" {
		a, err := strconv.Atoi(s)
		if err != nil {
			return nil, errors.Trace(err)
		}
		log.Info("Setting max-message-bytes", zap.Int("max-message-bytes", a))
		result.maxMessageBytes = a
	}

	if s := params.Get("max-batch-size"); s != "" {
		a, err := strconv.Atoi(s)
		if err != nil {
			return nil, errors.Trace(err)
		}
		log.Info("Setting max-batch-size", zap.Int("max-batch-size", a))
		result.maxBatchSize = a
	}

	if s := params.Get("protocol"); s != "" {
		result.protocol = s
	}

	if s := params.Get("enable-tidb-extension"); s != "" {
		b, err := strconv.ParseBool(s)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if b && result.protocol != "canal-json" {
			return nil, errors.Errorf("enable-tidb-extension only support Canal-JSON")
		}

		result.EnableTiDBExtension = b
	}

	return result, nil
}

func (c *Config) WithProtocol(protocol string) *Config {
	c.protocol = protocol
	return c
}

func (c *Config) WithTimezone(timezone string) *Config {
	c.timezone = timezone
	return c
}

func (c *Config) WithPartitionNum(partitions int32) *Config {
	c.PartitionNum = partitions
	return c
}

func (c *Config) WithChangefeedID(id string) *Config {
	c.changefeedID = id
	return c
}

// NewSaramaConfig can be used to initialize a sarama kafka consumer.
func NewSaramaConfig(version string, credential *security.Credential) (*sarama.Config, error) {
	config := sarama.NewConfig()

	v, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		return nil, errors.Trace(err)
	}

	config.ClientID = "ticdc_kafka_sarama_consumer"
	config.Version = v

	config.Metadata.Retry.Max = 10000
	config.Metadata.Retry.Backoff = 500 * time.Millisecond
	config.Consumer.Retry.Backoff = 500 * time.Millisecond
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	if credential != nil {
		if config.Net.TLS.Config, err = credential.ToTLSConfig(); err != nil {
			return nil, errors.Trace(err)
		}
		config.Net.TLS.Enable = true
	}

	return config, err
}
