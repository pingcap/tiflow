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

package kafka

import (
	"context"
	"crypto/tls"
	"math/rand"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"go.uber.org/zap"
)

var (
	defaultKafkaVersion = sarama.V2_0_0_0
	maxKafkaVersion     = sarama.V2_8_0_0
)

// NewSaramaConfig return the default config and set the according version and metrics
func NewSaramaConfig(ctx context.Context, o *Options) (*sarama.Config, error) {
	config := sarama.NewConfig()
	config.ClientID = o.ClientID
	var err error
	// Admin client would refresh metadata periodically,
	// if metadata cannot be refreshed easily, this would indicate the network condition between the
	// capture server and kafka broker is not good.
	// Set the timeout to 2 minutes to ensure that the underlying client does not retry for too long.
	// If retrying to obtain the metadata fails, simply return the error and let sinkManager rebuild the sink.
	config.Metadata.Retry.Max = 10
	config.Metadata.Retry.Backoff = 200 * time.Millisecond
	config.Metadata.Timeout = 2 * time.Minute

	config.Admin.Retry.Max = 10
	config.Admin.Retry.Backoff = 200 * time.Millisecond
	// This timeout control the request timeout for each admin request.
	// set it as the read timeout.
	config.Admin.Timeout = 10 * time.Second

	// Producer.Retry take effect when the producer try to send message to kafka
	// brokers. If kafka cluster is healthy, just the default value should be enough.
	// For kafka cluster with a bad network condition, producer should not try to
	// waster too much time on sending a message, get response no matter success
	// or fail as soon as possible is preferred.
	config.Producer.Retry.Max = 3
	config.Producer.Retry.Backoff = 100 * time.Millisecond

	// make sure sarama producer flush messages as soon as possible.
	config.Producer.Flush.Bytes = 0
	config.Producer.Flush.Messages = 0
	config.Producer.Flush.Frequency = time.Duration(0)
	config.Producer.Flush.MaxMessages = o.MaxMessages

	config.Net.DialTimeout = o.DialTimeout
	config.Net.WriteTimeout = o.WriteTimeout
	config.Net.ReadTimeout = o.ReadTimeout

	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.MaxMessageBytes = o.MaxMessageBytes
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.RequiredAcks(o.RequiredAcks)
	compression := strings.ToLower(strings.TrimSpace(o.Compression))
	switch compression {
	case "none":
		config.Producer.Compression = sarama.CompressionNone
	case "gzip":
		config.Producer.Compression = sarama.CompressionGZIP
	case "snappy":
		config.Producer.Compression = sarama.CompressionSnappy
	case "lz4":
		config.Producer.Compression = sarama.CompressionLZ4
	case "zstd":
		config.Producer.Compression = sarama.CompressionZSTD
	default:
		log.Warn("Unsupported compression algorithm", zap.String("compression", o.Compression))
		config.Producer.Compression = sarama.CompressionNone
	}
	if config.Producer.Compression != sarama.CompressionNone {
		log.Info("Kafka producer uses " + compression + " compression algorithm")
	}

	if o.EnableTLS {
		// for SSL encryption with a trust CA certificate, we must populate the
		// following two params of config.Net.TLS
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{
			MinVersion: tls.VersionTLS12,
			NextProtos: []string{"h2", "http/1.1"},
		}

		// for SSL encryption with self-signed CA certificate, we reassign the
		// config.Net.TLS.Config using the relevant credential files.
		if o.Credential != nil && o.Credential.IsTLSEnabled() {
			config.Net.TLS.Config, err = o.Credential.ToTLSConfig()
			if err != nil {
				return nil, errors.Trace(err)
			}
		}

		config.Net.TLS.Config.InsecureSkipVerify = o.InsecureSkipVerify
	}

	err = completeSaramaSASLConfig(ctx, config, o)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	kafkaVersion, err := getKafkaVersion(config, o)
	if err != nil {
		log.Warn("Can't get Kafka version by broker. ticdc will use default version",
			zap.String("defaultVersion", kafkaVersion.String()))
	}
	config.Version = kafkaVersion

	if o.IsAssignedVersion {
		version, err := sarama.ParseKafkaVersion(o.Version)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrKafkaInvalidVersion, err)
		}
		config.Version = version
		if !version.IsAtLeast(maxKafkaVersion) && version.String() != kafkaVersion.String() {
			log.Warn("The Kafka version you assigned may not be correct. "+
				"Please assign a version equal to or less than the specified version",
				zap.String("assignedVersion", version.String()),
				zap.String("desiredVersion", kafkaVersion.String()))
		}
	}
	return config, nil
}

func completeSaramaSASLConfig(ctx context.Context, config *sarama.Config, o *Options) error {
	if o.SASL != nil && o.SASL.SASLMechanism != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.Mechanism = sarama.SASLMechanism(o.SASL.SASLMechanism)
		switch o.SASL.SASLMechanism {
		case SASLTypeSCRAMSHA256, SASLTypeSCRAMSHA512, SASLTypePlaintext:
			config.Net.SASL.User = o.SASL.SASLUser
			config.Net.SASL.Password = o.SASL.SASLPassword
			if strings.EqualFold(string(o.SASL.SASLMechanism), SASLTypeSCRAMSHA256) {
				config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
					return &security.XDGSCRAMClient{HashGeneratorFcn: security.SHA256}
				}
			} else if strings.EqualFold(string(o.SASL.SASLMechanism), SASLTypeSCRAMSHA512) {
				config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
					return &security.XDGSCRAMClient{HashGeneratorFcn: security.SHA512}
				}
			}
		case SASLTypeGSSAPI:
			config.Net.SASL.GSSAPI.AuthType = int(o.SASL.GSSAPI.AuthType)
			config.Net.SASL.GSSAPI.Username = o.SASL.GSSAPI.Username
			config.Net.SASL.GSSAPI.ServiceName = o.SASL.GSSAPI.ServiceName
			config.Net.SASL.GSSAPI.KerberosConfigPath = o.SASL.GSSAPI.KerberosConfigPath
			config.Net.SASL.GSSAPI.Realm = o.SASL.GSSAPI.Realm
			config.Net.SASL.GSSAPI.DisablePAFXFAST = o.SASL.GSSAPI.DisablePAFXFAST
			switch o.SASL.GSSAPI.AuthType {
			case security.UserAuth:
				config.Net.SASL.GSSAPI.Password = o.SASL.GSSAPI.Password
			case security.KeyTabAuth:
				config.Net.SASL.GSSAPI.KeyTabPath = o.SASL.GSSAPI.KeyTabPath
			}

		case SASLTypeOAuth:
			p, err := newTokenProvider(ctx, o)
			if err != nil {
				return errors.Trace(err)
			}
			config.Net.SASL.TokenProvider = p
		}
	}

	return nil
}

func getKafkaVersion(config *sarama.Config, o *Options) (sarama.KafkaVersion, error) {
	var err error
	version := defaultKafkaVersion
	addrs := o.BrokerEndpoints
	if len(addrs) > 1 {
		// Shuffle the list of addresses to randomize the order in which
		// connections are attempted. This prevents routing all connections
		// to the first broker (which will usually succeed).
		rand.Shuffle(len(addrs), func(i, j int) {
			addrs[i], addrs[j] = addrs[j], addrs[i]
		})
	}
	for i := range addrs {
		version, err := getKafkaVersionFromBroker(config, o.RequestVersion, addrs[i])
		if err == nil {
			return version, err
		}
	}
	return version, err
}

func getKafkaVersionFromBroker(config *sarama.Config, requestVersion int16, addr string) (sarama.KafkaVersion, error) {
	KafkaVersion := defaultKafkaVersion
	broker := sarama.NewBroker(addr)
	err := broker.Open(config)
	defer func() {
		broker.Close()
	}()
	if err != nil {
		log.Warn("Kafka fail to open broker", zap.String("addr", addr), zap.Error(err))
		return KafkaVersion, err
	}
	apiResponse, err := broker.ApiVersions(&sarama.ApiVersionsRequest{Version: requestVersion})
	if err != nil {
		log.Warn("Kafka fail to get ApiVersions", zap.String("addr", addr), zap.Error(err))
		return KafkaVersion, err
	}
	// ApiKey method
	// 0      Produce
	// 3      Metadata (default)
	version := apiResponse.ApiKeys[3].MaxVersion
	if version >= 10 {
		KafkaVersion = sarama.V2_8_0_0
	} else if version >= 9 {
		KafkaVersion = sarama.V2_4_0_0
	} else if version >= 8 {
		KafkaVersion = sarama.V2_3_0_0
	} else if version >= 7 {
		KafkaVersion = sarama.V2_1_0_0
	} else if version >= 6 {
		KafkaVersion = sarama.V2_0_0_0
	} else if version >= 5 {
		KafkaVersion = sarama.V1_0_0_0
	} else if version >= 3 {
		KafkaVersion = sarama.V0_11_0_0
	} else if version >= 2 {
		KafkaVersion = sarama.V0_10_1_0
	} else if version >= 1 {
		KafkaVersion = sarama.V0_10_0_0
	} else if version >= 0 {
		KafkaVersion = sarama.V0_8_2_0
	}
	return KafkaVersion, nil
}
