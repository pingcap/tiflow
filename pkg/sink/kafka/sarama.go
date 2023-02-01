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
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"go.uber.org/zap"
)

// NewSaramaConfig return the default config and set the according version and metrics
func NewSaramaConfig(ctx context.Context, o *Options) (*sarama.Config, error) {
	config := sarama.NewConfig()

	version, err := sarama.ParseKafkaVersion(o.Version)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidVersion, err)
	}
	var role string
	if contextutil.IsOwnerFromCtx(ctx) {
		role = "owner"
	} else {
		role = "processor"
	}
	captureAddr := contextutil.CaptureAddrFromCtx(ctx)
	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)

	config.ClientID, err = NewKafkaClientID(role, captureAddr, changefeedID, o.ClientID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	config.Version = version

	// Producer fetch metadata from brokers frequently, if metadata cannot be
	// refreshed easily, this would indicate the network condition between the
	// capture server and kafka broker is not good.
	// In the scenario that cannot get response from Kafka server, this default
	// setting can help to get response more quickly.
	config.Metadata.Retry.Max = 1
	config.Metadata.Retry.Backoff = 100 * time.Millisecond
	// This Timeout is useless if the `RefreshMetadata` time cost is less than it.
	config.Metadata.Timeout = 1 * time.Minute

	// Admin.Retry take effect on `ClusterAdmin` related operations,
	// only `CreateTopic` for cdc now. set the `Timeout` to `1m` to make CI stable.
	config.Admin.Retry.Max = 5
	config.Admin.Retry.Backoff = 100 * time.Millisecond
	config.Admin.Timeout = 1 * time.Minute

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
	config.Producer.RequiredAcks = sarama.WaitForAll
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
	}

	completeSaramaSASLConfig(config, o)

	return config, err
}

func completeSaramaSASLConfig(config *sarama.Config, o *Options) {
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
		}
	}
}
