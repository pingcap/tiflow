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
	"net/http"
	"net/url"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/gin-gonic/gin/binding"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"
)

func TestNewSaramaConfig(t *testing.T) {
	options := NewOptions()
	options.Version = "invalid"
	ctx := context.Background()
	_, err := NewSaramaConfig(ctx, options)
	require.Regexp(t, "invalid version.*", errors.Cause(err))
	options.Version = "2.6.0"

	options.ClientID = "test-kafka-client"
	compressionCases := []struct {
		algorithm string
		expected  sarama.CompressionCodec
	}{
		{"none", sarama.CompressionNone},
		{"gzip", sarama.CompressionGZIP},
		{"snappy", sarama.CompressionSnappy},
		{"lz4", sarama.CompressionLZ4},
		{"zstd", sarama.CompressionZSTD},
		{"others", sarama.CompressionNone},
	}
	for _, cc := range compressionCases {
		options.Compression = cc.algorithm
		cfg, err := NewSaramaConfig(ctx, options)
		require.NoError(t, err)
		require.Equal(t, cc.expected, cfg.Producer.Compression)
	}

	options.EnableTLS = true
	options.Credential = &security.Credential{
		CAPath:   "/invalid/ca/path",
		CertPath: "/invalid/cert/path",
		KeyPath:  "/invalid/key/path",
	}
	_, err = NewSaramaConfig(ctx, options)
	require.Regexp(t, ".*no such file or directory", errors.Cause(err))

	saslOptions := NewOptions()
	saslOptions.Version = "2.6.0"
	saslOptions.ClientID = "test-sasl-scram"
	saslOptions.SASL = &security.SASL{
		SASLUser:      "user",
		SASLPassword:  "password",
		SASLMechanism: sarama.SASLTypeSCRAMSHA256,
	}

	cfg, err := NewSaramaConfig(ctx, saslOptions)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Equal(t, "user", cfg.Net.SASL.User)
	require.Equal(t, "password", cfg.Net.SASL.Password)
	require.Equal(t, sarama.SASLMechanism("SCRAM-SHA-256"), cfg.Net.SASL.Mechanism)
}

func TestApplySASL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		URI           string
		replicaConfig func() *config.ReplicaConfig
		exceptErr     string
	}{
		{
			name:          "no params",
			URI:           "kafka://127.0.0.1:9092/abc",
			replicaConfig: config.GetDefaultReplicaConfig,
			exceptErr:     "",
		},
		{
			name: "valid PLAIN SASL",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-user=user&sasl-password=password&sasl-mechanism=plain",
			replicaConfig: config.GetDefaultReplicaConfig,
			exceptErr:     "",
		},
		{
			name: "valid SCRAM SASL",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-user=user&sasl-password=password&sasl-mechanism=SCRAM-SHA-512",
			replicaConfig: config.GetDefaultReplicaConfig,
			exceptErr:     "",
		},
		{
			name: "valid GSSAPI user auth SASL",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-mechanism=GSSAPI&sasl-gssapi-auth-type=USER" +
				"&sasl-gssapi-kerberos-config-path=/root/config" +
				"&sasl-gssapi-service-name=a&sasl-gssapi-user=user" +
				"&sasl-gssapi-password=pwd" +
				"&sasl-gssapi-realm=realm&sasl-gssapi-disable-pafxfast=false",
			replicaConfig: config.GetDefaultReplicaConfig,
			exceptErr:     "",
		},
		{
			name: "valid GSSAPI keytab auth SASL",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-mechanism=GSSAPI&sasl-gssapi-auth-type=keytab" +
				"&sasl-gssapi-kerberos-config-path=/root/config" +
				"&sasl-gssapi-service-name=a&sasl-gssapi-user=user" +
				"&sasl-gssapi-keytab-path=/root/keytab" +
				"&sasl-gssapi-realm=realm&sasl-gssapi-disable-pafxfast=false",
			replicaConfig: config.GetDefaultReplicaConfig,
			exceptErr:     "",
		},
		{
			name: "invalid mechanism",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-mechanism=a",
			replicaConfig: config.GetDefaultReplicaConfig,
			exceptErr:     "unknown a SASL mechanism",
		},
		{
			name: "invalid GSSAPI auth type",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-mechanism=gssapi&sasl-gssapi-auth-type=keyta1b",
			replicaConfig: config.GetDefaultReplicaConfig,
			exceptErr:     "unknown keyta1b auth type",
		},
		{
			name: "valid OAUTHBEARER SASL",
			URI:  "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0&sasl-mechanism=OAUTHBEARER",
			replicaConfig: func() *config.ReplicaConfig {
				cfg := config.GetDefaultReplicaConfig()
				oauthMechanism := string(security.OAuthMechanism)
				clientID := "client_id"
				clientSecret := "Y2xpZW50X3NlY3JldA==" // base64(client_secret)
				tokenURL := "127.0.0.1:9093/token"
				cfg.Sink.KafkaConfig = &config.KafkaConfig{
					SASLMechanism:         &oauthMechanism,
					SASLOAuthClientID:     &clientID,
					SASLOAuthClientSecret: &clientSecret,
					SASLOAuthTokenURL:     &tokenURL,
				}
				return cfg
			},
			exceptErr: "",
		},
		{
			name: "invalid OAUTHBEARER SASL: missing client id",
			URI:  "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0&sasl-mechanism=OAUTHBEARER",
			replicaConfig: func() *config.ReplicaConfig {
				cfg := config.GetDefaultReplicaConfig()
				oauthMechanism := string(security.OAuthMechanism)
				clientSecret := "Y2xpZW50X3NlY3JldA==" // base64(client_secret)
				tokenURL := "127.0.0.1:9093/token"
				cfg.Sink.KafkaConfig = &config.KafkaConfig{
					SASLMechanism:         &oauthMechanism,
					SASLOAuthClientSecret: &clientSecret,
					SASLOAuthTokenURL:     &tokenURL,
				}
				return cfg
			},
			exceptErr: "OAuth2 client id is empty",
		},
		{
			name: "invalid OAUTHBEARER SASL: missing client secret",
			URI:  "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0&sasl-mechanism=OAUTHBEARER",
			replicaConfig: func() *config.ReplicaConfig {
				cfg := config.GetDefaultReplicaConfig()
				oauthMechanism := string(security.OAuthMechanism)
				clientID := "client_id"
				tokenURL := "127.0.0.1:9093/token"
				cfg.Sink.KafkaConfig = &config.KafkaConfig{
					SASLMechanism:     &oauthMechanism,
					SASLOAuthClientID: &clientID,
					SASLOAuthTokenURL: &tokenURL,
				}
				return cfg
			},
			exceptErr: "OAuth2 client secret is empty",
		},
		{
			name: "invalid OAUTHBEARER SASL: missing token url",
			URI:  "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0&sasl-mechanism=OAUTHBEARER",
			replicaConfig: func() *config.ReplicaConfig {
				cfg := config.GetDefaultReplicaConfig()
				oauthMechanism := string(security.OAuthMechanism)
				clientID := "client_id"
				clientSecret := "Y2xpZW50X3NlY3JldA==" // base64(client_secret)
				cfg.Sink.KafkaConfig = &config.KafkaConfig{
					SASLMechanism:         &oauthMechanism,
					SASLOAuthClientID:     &clientID,
					SASLOAuthClientSecret: &clientSecret,
				}
				return cfg
			},
			exceptErr: "OAuth2 token url is empty",
		},
		{
			name: "invalid OAUTHBEARER SASL: non base64 client secret",
			URI:  "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0&sasl-mechanism=OAUTHBEARER",
			replicaConfig: func() *config.ReplicaConfig {
				cfg := config.GetDefaultReplicaConfig()
				oauthMechanism := string(security.OAuthMechanism)
				clientID := "client_id"
				clientSecret := "client_secret"
				tokenURL := "127.0.0.1:9093/token"
				cfg.Sink.KafkaConfig = &config.KafkaConfig{
					SASLMechanism:         &oauthMechanism,
					SASLOAuthClientID:     &clientID,
					SASLOAuthClientSecret: &clientSecret,
					SASLOAuthTokenURL:     &tokenURL,
				}
				return cfg
			},
			exceptErr: "OAuth2 client secret is not base64 encoded",
		},
		{
			name: "invalid OAUTHBEARER SASL: wrong mechanism",
			URI:  "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0&sasl-mechanism=GSSAPI",
			replicaConfig: func() *config.ReplicaConfig {
				cfg := config.GetDefaultReplicaConfig()
				oauthMechanism := string(security.OAuthMechanism)
				clientID := "client_id"
				clientSecret := "Y2xpZW50X3NlY3JldA==" // base64(client_secret)
				tokenURL := "127.0.0.1:9093/token"
				cfg.Sink.KafkaConfig = &config.KafkaConfig{
					SASLMechanism:         &oauthMechanism,
					SASLOAuthClientID:     &clientID,
					SASLOAuthClientSecret: &clientSecret,
					SASLOAuthTokenURL:     &tokenURL,
				}
				return cfg
			},
			exceptErr: "OAuth2 is only supported with SASL mechanism type OAUTHBEARER",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			options := NewOptions()
			sinkURI, err := url.Parse(test.URI)
			require.NoError(t, err)
			req := &http.Request{URL: sinkURI}
			urlParameter := &urlConfig{}
			err = binding.Query.Bind(req, urlParameter)
			require.NoError(t, err)
			if test.exceptErr == "" {
				require.Nil(t, options.applySASL(urlParameter, test.replicaConfig()))
			} else {
				require.Regexp(t, test.exceptErr,
					options.applySASL(urlParameter, test.replicaConfig()).Error())
			}
		})
	}
}

func TestApplyTLS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		URI        string
		tlsEnabled bool
		exceptErr  string
	}{
		{
			name: "tls config with 'enable-tls' set to true",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-user=user&sasl-password=password&sasl-mechanism=plain&enable-tls=true",
			tlsEnabled: true,
			exceptErr:  "",
		},
		{
			name: "tls config with no 'enable-tls', and credential files are supplied",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-user=user&sasl-password=password&sasl-mechanism=plain" +
				"&ca=/root/ca.file&cert=/root/cert.file&key=/root/key.file",
			tlsEnabled: true,
			exceptErr:  "",
		},
		{
			name: "tls config with no 'enable-tls', and credential files are not supplied",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-user=user&sasl-password=password&sasl-mechanism=plain",
			tlsEnabled: false,
			exceptErr:  "",
		},
		{
			name: "tls config with 'enable-tls' set to false, and credential files are supplied",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-user=user&sasl-password=password&sasl-mechanism=plain&enable-tls=false" +
				"&ca=/root/ca&cert=/root/cert&key=/root/key",
			tlsEnabled: false,
			exceptErr:  "credential files are supplied, but 'enable-tls' is set to false",
		},
		{
			name: "tls config with 'enable-tls' set to true, and some of " +
				"the credential files are not supplied ",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-user=user&sasl-password=password&sasl-mechanism=plain&enable-tls=true" +
				"&ca=/root/ca&cert=/root/cert&",
			tlsEnabled: false,
			exceptErr:  "ca, cert and key files should all be supplied",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			options := NewOptions()
			sinkURI, err := url.Parse(test.URI)
			require.NoError(t, err)
			req := &http.Request{URL: sinkURI}
			urlParameter := &urlConfig{}
			err = binding.Query.Bind(req, urlParameter)
			require.NoError(t, err)
			if test.exceptErr == "" {
				require.Nil(t, options.applyTLS(urlParameter))
			} else {
				require.Regexp(t, test.exceptErr, options.applyTLS(urlParameter).Error())
			}
			require.Equal(t, test.tlsEnabled, options.EnableTLS)
		})
	}
}

func TestCompleteSaramaSASLConfig(t *testing.T) {
	t.Parallel()

	// Test that SASL is turned on correctly.
	options := NewOptions()
	options.SASL = &security.SASL{
		SASLUser:      "user",
		SASLPassword:  "password",
		SASLMechanism: "",
		GSSAPI:        security.GSSAPI{},
	}
	ctx := context.Background()
	saramaConfig := sarama.NewConfig()
	completeSaramaSASLConfig(ctx, saramaConfig, options)
	require.False(t, saramaConfig.Net.SASL.Enable)
	options.SASL.SASLMechanism = "plain"
	completeSaramaSASLConfig(ctx, saramaConfig, options)
	require.True(t, saramaConfig.Net.SASL.Enable)
	// Test that the SCRAMClientGeneratorFunc is set up correctly.
	options = NewOptions()
	options.SASL = &security.SASL{
		SASLUser:      "user",
		SASLPassword:  "password",
		SASLMechanism: "plain",
		GSSAPI:        security.GSSAPI{},
	}
	saramaConfig = sarama.NewConfig()
	completeSaramaSASLConfig(ctx, saramaConfig, options)
	require.Nil(t, saramaConfig.Net.SASL.SCRAMClientGeneratorFunc)
	options.SASL.SASLMechanism = "SCRAM-SHA-512"
	completeSaramaSASLConfig(ctx, saramaConfig, options)
	require.NotNil(t, saramaConfig.Net.SASL.SCRAMClientGeneratorFunc)
}

func TestSaramaTimeout(t *testing.T) {
	options := NewOptions()
	saramaConfig, err := NewSaramaConfig(context.Background(), options)
	require.NoError(t, err)
	require.Equal(t, options.DialTimeout, saramaConfig.Net.DialTimeout)
	require.Equal(t, options.WriteTimeout, saramaConfig.Net.WriteTimeout)
	require.Equal(t, options.ReadTimeout, saramaConfig.Net.ReadTimeout)
}
