// Copyright 2023 PingCAP, Inc.
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

package v2

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	kafkaV1 "github.com/pingcap/tiflow/cdc/sink/mq/producer/kafka"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type Config struct {
	clientID     string
	maxRetry     int
	retryBackOff time.Duration

	dialTimeout  time.Duration
	writeTimeout time.Duration
	readTimeout  time.Duration

	maxMessageBytes int
	requireAck      kafka.RequiredAcks

	compression kafka.Compression

	autoCreate bool

	// credential
	// sasl
	// enabletls
}

var (
	validClientID     = regexp.MustCompile(`\A[A-Za-z0-9._-]+\z`)
	commonInvalidChar = regexp.MustCompile(`[\?:,"]`)
)

const (
	defaultMaxRetry     = 3
	defaultRetryBackOff = 100 * time.Millisecond
	defaultTimeout      = 100 * time.Millisecond
)

func buildKafkaClientID(role, captureAddr string, changefeedID model.ChangeFeedID, clientID string) (string, error) {
	if clientID == "" {
		clientID = fmt.Sprintf("TiCDC_producer_%s_%s_%s_%s",
			role, captureAddr, changefeedID.Namespace, changefeedID.ID)
	}
	if !validClientID.MatchString(clientID) {
		return "", cerror.ErrKafkaInvalidClientID.GenWithStackByArgs(clientID)
	}
	return clientID, nil
}

func NewConfig(baseConfig *kafkaV1.Config, role, captureAddr string, changefeedID model.ChangeFeedID) (*Config, error) {
	clientID, err := buildKafkaClientID(role, captureAddr, changefeedID, baseConfig.ClientID)
	if err != nil {
		return nil, err
	}

	result := &Config{
		clientID:     clientID,
		maxRetry:     defaultMaxRetry,
		retryBackOff: defaultRetryBackOff,

		dialTimeout:  defaultTimeout,
		writeTimeout: defaultTimeout,
		readTimeout:  defaultTimeout,

		maxMessageBytes: baseConfig.MaxMessageBytes,

		requireAck: kafka.RequireAll,
	}

	compression := strings.ToLower(strings.TrimSpace(baseConfig.Compression))
	switch compression {
	case "none":
		// for none, do nothing, it's the default.
	case "gzip":
		result.compression = kafka.Gzip
	case "snappy":
		result.compression = kafka.Snappy
	case "lz4":
		result.compression = kafka.Lz4
	case "zstd":
		result.compression = kafka.Zstd
	default:
		log.Warn("Unsupported compression algorithm",
			zap.String("compression", baseConfig.Compression))
	}
	if result.compression != 0 {
		log.Info("Kafka producer uses " + compression + " compression algorithm")
	}

	if baseConfig.EnableTLS {
		// how to handle enable tls ?
	}

	if baseConfig.SASL != nil && baseConfig.SASL.SASLMechanism != "" {
		// how to handle sasl
	}

	return result, nil
}
