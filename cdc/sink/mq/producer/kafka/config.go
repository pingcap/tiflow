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

package kafka

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"go.uber.org/zap"
)

// Config stores user specified Kafka producer configuration
type Config struct {
	BrokerEndpoints []string
	PartitionNum    int32

	// User should make sure that `replication-factor` not greater than the number of kafka brokers.
	ReplicationFactor int16

	Version         string
	MaxMessageBytes int
	Compression     string
	ClientID        string
	EnableTLS       bool
	Credential      *security.Credential
	SASL            *security.SASL
	// control whether to create topic
	AutoCreate bool

	// Timeout for sarama `config.Net` configurations, default to `10s`
	DialTimeout  time.Duration
	WriteTimeout time.Duration
	ReadTimeout  time.Duration
}

// NewConfig returns a default Kafka configuration
func NewConfig() *Config {
	return &Config{
		Version: "2.4.0",
		// MaxMessageBytes will be used to initialize producer
		MaxMessageBytes:   config.DefaultMaxMessageBytes,
		ReplicationFactor: 1,
		Compression:       "none",
		Credential:        &security.Credential{},
		SASL:              &security.SASL{},
		AutoCreate:        true,
		DialTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		ReadTimeout:       10 * time.Second,
	}
}

// set the partition-num by the topic's partition count.
func (c *Config) setPartitionNum(realPartitionCount int32) error {
	// user does not specify the `partition-num` in the sink-uri
	if c.PartitionNum == 0 {
		c.PartitionNum = realPartitionCount
		log.Info("partitionNum is not set, set by topic's partition-num",
			zap.Int32("partitionNum", realPartitionCount))
		return nil
	}

	if c.PartitionNum < realPartitionCount {
		log.Warn("number of partition specified in sink-uri is less than that of the actual topic. "+
			"Some partitions will not have messages dispatched to",
			zap.Int32("sinkUriPartitions", c.PartitionNum),
			zap.Int32("topicPartitions", realPartitionCount))
		return nil
	}

	// Make sure that the user-specified `partition-num` is not greater than
	// the real partition count, since messages would be dispatched to different
	// partitions, this could prevent potential correctness problems.
	if c.PartitionNum > realPartitionCount {
		return cerror.ErrKafkaInvalidPartitionNum.GenWithStack(
			"the number of partition (%d) specified in sink-uri is more than that of actual topic (%d)",
			c.PartitionNum, realPartitionCount)
	}
	return nil
}

// Apply the configuration to the sarama producer.
func (c *Config) Apply(sinkURI *url.URL, replicaConfig *config.ReplicaConfig) error {
	c.BrokerEndpoints = strings.Split(sinkURI.Host, ",")
	params := sinkURI.Query()
	s := params.Get("partition-num")
	if s != "" {
		a, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return err
		}
		c.PartitionNum = int32(a)
		if c.PartitionNum <= 0 {
			return cerror.ErrKafkaInvalidPartitionNum.GenWithStackByArgs(c.PartitionNum)
		}
	}

	s = params.Get("replication-factor")
	if s != "" {
		a, err := strconv.ParseInt(s, 10, 16)
		if err != nil {
			return err
		}
		c.ReplicationFactor = int16(a)
	}

	s = params.Get("kafka-version")
	if s != "" {
		c.Version = s
	}

	s = params.Get("max-message-bytes")
	if s != "" {
		a, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		c.MaxMessageBytes = a
	}

	s = params.Get("compression")
	if s != "" {
		c.Compression = s
	}

	c.ClientID = params.Get("kafka-client-id")

	s = params.Get("auto-create-topic")
	if s != "" {
		autoCreate, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		c.AutoCreate = autoCreate
	}

	s = params.Get("dial-timeout")
	if s != "" {
		a, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		c.DialTimeout = a
	}

	s = params.Get("write-timeout")
	if s != "" {
		a, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		c.WriteTimeout = a
	}

	s = params.Get("read-timeout")
	if s != "" {
		a, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		c.ReadTimeout = a
	}

	err := c.applySASL(params, replicaConfig)
	if err != nil {
		return err
	}

	err = c.applyTLS(params)
	if err != nil {
		return err
	}

	return nil
}

func (c *Config) applyTLS(params url.Values) error {
	s := params.Get("ca")
	if s != "" {
		c.Credential.CAPath = s
	}

	s = params.Get("cert")
	if s != "" {
		c.Credential.CertPath = s
	}

	s = params.Get("key")
	if s != "" {
		c.Credential.KeyPath = s
	}

	if c.Credential != nil && !c.Credential.IsEmpty() &&
		!c.Credential.IsTLSEnabled() {
		return cerror.WrapError(cerror.ErrKafkaInvalidConfig,
			errors.New("ca, cert and key files should all be supplied"))
	}

	// if enable-tls is not set, but credential files are set,
	//    then tls should be enabled, and the self-signed CA certificate is used.
	// if enable-tls is set to true, and credential files are not set,
	//	  then tls should be enabled, and the trusted CA certificate on OS is used.
	// if enable-tls is set to false, and credential files are set,
	//	  then an error is returned.
	s = params.Get("enable-tls")
	if s != "" {
		enableTLS, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}

		if c.Credential != nil && c.Credential.IsTLSEnabled() && !enableTLS {
			return cerror.WrapError(cerror.ErrKafkaInvalidConfig,
				errors.New("credential files are supplied, but 'enable-tls' is set to false"))
		}
		c.EnableTLS = enableTLS
	} else {
		if c.Credential != nil && c.Credential.IsTLSEnabled() {
			c.EnableTLS = true
		}
	}

	return nil
}

func (c *Config) applySASL(params url.Values, replicaConfig *config.ReplicaConfig) error {
	s := params.Get("sasl-user")
	if s != "" {
		c.SASL.SASLUser = s
	}

	s = params.Get("sasl-password")
	if s != "" {
		c.SASL.SASLPassword = s
	}

	s = params.Get("sasl-mechanism")
	if s != "" {
		mechanism, err := security.SASLMechanismFromString(s)
		if err != nil {
			return cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
		}
		c.SASL.SASLMechanism = mechanism
	} else if replicaConfig != nil && replicaConfig.Sink != nil && replicaConfig.Sink.KafkaConfig != nil && replicaConfig.Sink.KafkaConfig.SASLMechanism != nil {
		mechanism, err := security.SASLMechanismFromString(*replicaConfig.Sink.KafkaConfig.SASLMechanism)
		if err != nil {
			return cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
		}
		c.SASL.SASLMechanism = mechanism
	}

	s = params.Get("sasl-gssapi-auth-type")
	if s != "" {
		authType, err := security.AuthTypeFromString(s)
		if err != nil {
			return cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
		}
		c.SASL.GSSAPI.AuthType = authType
	}

	s = params.Get("sasl-gssapi-keytab-path")
	if s != "" {
		c.SASL.GSSAPI.KeyTabPath = s
	}

	s = params.Get("sasl-gssapi-kerberos-config-path")
	if s != "" {
		c.SASL.GSSAPI.KerberosConfigPath = s
	}

	s = params.Get("sasl-gssapi-service-name")
	if s != "" {
		c.SASL.GSSAPI.ServiceName = s
	}

	s = params.Get("sasl-gssapi-user")
	if s != "" {
		c.SASL.GSSAPI.Username = s
	}

	s = params.Get("sasl-gssapi-password")
	if s != "" {
		c.SASL.GSSAPI.Password = s
	}

	s = params.Get("sasl-gssapi-realm")
	if s != "" {
		c.SASL.GSSAPI.Realm = s
	}

	s = params.Get("sasl-gssapi-disable-pafxfast")
	if s != "" {
		disablePAFXFAST, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		c.SASL.GSSAPI.DisablePAFXFAST = disablePAFXFAST
	}

	if replicaConfig.Sink != nil && replicaConfig.Sink.KafkaConfig != nil {
		if replicaConfig.Sink.KafkaConfig.SASLOAuthClientID != nil {
			clientID := *replicaConfig.Sink.KafkaConfig.SASLOAuthClientID
			if clientID == "" {
				return cerror.ErrKafkaInvalidConfig.GenWithStack("OAuth2 client ID cannot be empty")
			}
			c.SASL.OAuth2.ClientID = clientID
		}

		if replicaConfig.Sink.KafkaConfig.SASLOAuthClientSecret != nil {
			clientSecret := *replicaConfig.Sink.KafkaConfig.SASLOAuthClientSecret
			if clientSecret == "" {
				return cerror.ErrKafkaInvalidConfig.GenWithStack(
					"OAuth2 client secret cannot be empty")
			}

			// BASE64 decode the client secret
			decodedClientSecret, err := base64.StdEncoding.DecodeString(clientSecret)
			if err != nil {
				log.Error("OAuth2 client secret is not base64 encoded", zap.Error(err))
				return cerror.ErrKafkaInvalidConfig.GenWithStack(
					"OAuth2 client secret is not base64 encoded")
			}
			c.SASL.OAuth2.ClientSecret = string(decodedClientSecret)
		}

		if replicaConfig.Sink.KafkaConfig.SASLOAuthTokenURL != nil {
			tokenURL := *replicaConfig.Sink.KafkaConfig.SASLOAuthTokenURL
			if tokenURL == "" {
				return cerror.ErrKafkaInvalidConfig.GenWithStack(
					"OAuth2 token URL cannot be empty")
			}
			c.SASL.OAuth2.TokenURL = tokenURL
		}

		if c.SASL.OAuth2.IsEnable() {
			if c.SASL.SASLMechanism != security.OAuthMechanism {
				return cerror.ErrKafkaInvalidConfig.GenWithStack(
					"OAuth2 is only supported with SASL mechanism type OAUTHBEARER, but got %s",
					c.SASL.SASLMechanism)
			}

			if err := c.SASL.OAuth2.Validate(); err != nil {
				return cerror.ErrKafkaInvalidConfig.Wrap(err)
			}
			c.SASL.OAuth2.SetDefault()
		}

		if replicaConfig.Sink.KafkaConfig.SASLOAuthScopes != nil {
			c.SASL.OAuth2.Scopes = replicaConfig.Sink.KafkaConfig.SASLOAuthScopes
		}

		if replicaConfig.Sink.KafkaConfig.SASLOAuthGrantType != nil {
			c.SASL.OAuth2.GrantType = *replicaConfig.Sink.KafkaConfig.SASLOAuthGrantType
		}

		if replicaConfig.Sink.KafkaConfig.SASLOAuthAudience != nil {
			c.SASL.OAuth2.Audience = *replicaConfig.Sink.KafkaConfig.SASLOAuthAudience
		}
	}

	return nil
}

// AutoCreateTopicConfig is used to create topic configuration.
type AutoCreateTopicConfig struct {
	AutoCreate        bool
	PartitionNum      int32
	ReplicationFactor int16
}

// DeriveTopicConfig derive a `topicConfig` from the `Config`
func (c *Config) DeriveTopicConfig() *AutoCreateTopicConfig {
	return &AutoCreateTopicConfig{
		AutoCreate:        c.AutoCreate,
		PartitionNum:      c.PartitionNum,
		ReplicationFactor: c.ReplicationFactor,
	}
}

// NewSaramaConfig return the default config and set the according version and metrics
func NewSaramaConfig(ctx context.Context, c *Config) (*sarama.Config, error) {
	config := sarama.NewConfig()

	version, err := sarama.ParseKafkaVersion(c.Version)
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

	config.ClientID, err = kafkaClientID(role, captureAddr, changefeedID, c.ClientID)
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

	config.Net.DialTimeout = c.DialTimeout
	config.Net.WriteTimeout = c.WriteTimeout
	config.Net.ReadTimeout = c.ReadTimeout

	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.MaxMessageBytes = c.MaxMessageBytes
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	compression := strings.ToLower(strings.TrimSpace(c.Compression))
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
		log.Warn("Unsupported compression algorithm", zap.String("compression", c.Compression))
		config.Producer.Compression = sarama.CompressionNone
	}
	if config.Producer.Compression != sarama.CompressionNone {
		log.Info("Kafka producer uses " + compression + " compression algorithm")
	}

	if c.EnableTLS {
		// for SSL encryption with a trust CA certificate, we must populate the
		// following two params of config.Net.TLS
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{
			MinVersion: tls.VersionTLS12,
			NextProtos: []string{"h2", "http/1.1"},
		}

		// for SSL encryption with self-signed CA certificate, we reassign the
		// config.Net.TLS.Config using the relevant credential files.
		if c.Credential != nil && c.Credential.IsTLSEnabled() {
			config.Net.TLS.Config, err = c.Credential.ToTLSConfig()
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}

	if err := completeSaramaSASLConfig(ctx, config, c); err != nil {
		return nil, errors.Trace(err)
	}

	return config, err
}

func completeSaramaSASLConfig(ctx context.Context, config *sarama.Config, c *Config) error {
	if c.SASL != nil && c.SASL.SASLMechanism != "" {
		config.Net.SASL.Enable = true
		config.Net.SASL.Mechanism = sarama.SASLMechanism(c.SASL.SASLMechanism)
		switch c.SASL.SASLMechanism {
		case sarama.SASLTypeSCRAMSHA256, sarama.SASLTypeSCRAMSHA512, sarama.SASLTypePlaintext:
			config.Net.SASL.User = c.SASL.SASLUser
			config.Net.SASL.Password = c.SASL.SASLPassword
			if strings.EqualFold(string(c.SASL.SASLMechanism), sarama.SASLTypeSCRAMSHA256) {
				config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
					return &security.XDGSCRAMClient{HashGeneratorFcn: security.SHA256}
				}
			} else if strings.EqualFold(string(c.SASL.SASLMechanism), sarama.SASLTypeSCRAMSHA512) {
				config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
					return &security.XDGSCRAMClient{HashGeneratorFcn: security.SHA512}
				}
			}
		case sarama.SASLTypeGSSAPI:
			config.Net.SASL.GSSAPI.AuthType = int(c.SASL.GSSAPI.AuthType)
			config.Net.SASL.GSSAPI.Username = c.SASL.GSSAPI.Username
			config.Net.SASL.GSSAPI.ServiceName = c.SASL.GSSAPI.ServiceName
			config.Net.SASL.GSSAPI.KerberosConfigPath = c.SASL.GSSAPI.KerberosConfigPath
			config.Net.SASL.GSSAPI.Realm = c.SASL.GSSAPI.Realm
			config.Net.SASL.GSSAPI.DisablePAFXFAST = c.SASL.GSSAPI.DisablePAFXFAST
			switch c.SASL.GSSAPI.AuthType {
			case security.UserAuth:
				config.Net.SASL.GSSAPI.Password = c.SASL.GSSAPI.Password
			case security.KeyTabAuth:
				config.Net.SASL.GSSAPI.KeyTabPath = c.SASL.GSSAPI.KeyTabPath
			}
		case sarama.SASLTypeOAuth:
			p, err := newTokenProvider(ctx, c)
			if err != nil {
				return errors.Trace(err)
			}
			config.Net.SASL.TokenProvider = p
		}

	}
	return nil
}
