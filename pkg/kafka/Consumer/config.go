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
	protocol      string
	changefeedID  string
}

type option func(c *Config)

func WithProtocol(protocol string) option {
	return func(c *Config) {
		c.protocol = protocol
	}
}

func WithTimezone(timezone string) option {
	return func(c *Config) {
		c.timezone = timezone
	}
}

func WithPartitionNum(partitions int32) option {
	return func(c *Config) {
		c.PartitionNum = partitions
	}
}

// NewConfig return a default `Config`
func NewConfig() *Config {
	return &Config{
		timezone:        "system",
		Version:         "2.4.0",
		GroupID:         fmt.Sprintf("ticdc_kafka_consumer_%s", uuid.New().String()),
		maxMessageBytes: math.MaxInt,
		maxBatchSize:    math.MaxInt,
	}
}

func (c *Config) Initialize(upstream, downstream string, opts ...option) error {
	if upstream == "" || downstream == "" {
		return errors.Errorf("upstream-url or downstream-url not found")
	}

	uri, err := url.Parse(upstream)
	if err != nil {
		return errors.Trace(err)
	}

	c.downstreamStr = downstream

	for _, opt := range opts {
		opt(c)
	}

	scheme := strings.ToLower(uri.Scheme)
	if scheme != "kafka" {
		return errors.Errorf("scheme is not kafka, but %v", scheme)
	}

	endPoints := strings.Split(uri.Host, ",")
	if len(endPoints) == 0 {
		return errors.New("kafka broker addresses not found")
	}
	c.BrokerEndpoints = endPoints

	topic := strings.TrimFunc(uri.Path, func(r rune) bool {
		return r == '/'
	})
	if topic == "" {
		return errors.New("topic should be given")
	}
	c.Topic = topic

	params := uri.Query()
	if s := params.Get("version"); s != "" {
		c.Version = s
	}
	if s := params.Get("consumer-group-id"); s != "" {
		c.GroupID = s
	}

	if s := params.Get("max-message-bytes"); s != "" {
		a, err := strconv.Atoi(s)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("Setting max-message-bytes", zap.Int("max-message-bytes", a))
		c.maxMessageBytes = a
	}

	if s := params.Get("max-batch-size"); s != "" {
		a, err := strconv.Atoi(s)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("Setting max-batch-size", zap.Int("max-batch-size", a))
		c.maxBatchSize = a
	}
	return nil
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
