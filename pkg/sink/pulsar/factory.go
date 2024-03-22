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

package pulsar

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsar/auth"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/metrics/mq"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// FactoryCreator defines the type of factory creator.
type FactoryCreator func(config *config.PulsarConfig, changefeedID model.ChangeFeedID, sinkConfig *config.SinkConfig) (pulsar.Client, error)

// NewCreatorFactory returns a factory implemented based on kafka-go
func NewCreatorFactory(config *config.PulsarConfig, changefeedID model.ChangeFeedID, sinkConfig *config.SinkConfig) (pulsar.Client, error) {
	option := pulsar.ClientOptions{
		URL: config.BrokerURL,
		CustomMetricsLabels: map[string]string{
			"changefeed": changefeedID.ID,
			"namespace":  changefeedID.Namespace,
		},
		ConnectionTimeout: config.ConnectionTimeout.Duration(),
		OperationTimeout:  config.OperationTimeout.Duration(),
		// add pulsar default metrics
		MetricsRegisterer: mq.GetMetricRegistry(),
		Logger:            NewPulsarLogger(log.L()),
	}
	log.Info("pulsar client factory created",
		zap.Stringer("changefeedID", changefeedID),
		zap.Any("clientOptions", option))

	var err error
	// ismTLSAuthentication is true if it is mTLS authentication
	var ismTLSAuthentication bool
	ismTLSAuthentication, option.Authentication, err = setupAuthentication(config)
	if err != nil {
		log.Error("setup pulsar authentication fail", zap.Error(err))
		return nil, err
	}
	// When mTLS authentication is enabled, trust certs file path is required.
	if ismTLSAuthentication {
		if sinkConfig.PulsarConfig != nil && sinkConfig.PulsarConfig.TLSTrustCertsFilePath != nil {
			option.TLSTrustCertsFilePath = *sinkConfig.PulsarConfig.TLSTrustCertsFilePath
		} else {
			return nil, cerror.ErrPulsarInvalidConfig.
				GenWithStackByArgs("pulsar tls trust certs file path is not set when mTLS authentication is enabled")
		}
	}

	// Check and set pulsar TLS config
	if sinkConfig.PulsarConfig != nil {
		sinkPulsar := sinkConfig.PulsarConfig
		// Note(dongmen): If pulsar cluster set `tlsRequireTrustedClientCertOnConnect=false`,
		// provide the TLS trust certificate file is enough.
		if sinkPulsar.TLSTrustCertsFilePath != nil {
			option.TLSTrustCertsFilePath = *sinkPulsar.TLSTrustCertsFilePath
			log.Info("pulsar tls trust certificate file is set, tls encryption enable")
		}
		// Note(dongmen): If pulsar cluster set `tlsRequireTrustedClientCertOnConnect=true`,
		// then the client must set the TLS certificate and key.
		// Otherwise, a error like "remote error: tls: certificate required" will be returned.
		if sinkPulsar.TLSCertificateFile != nil && sinkPulsar.TLSKeyFilePath != nil {
			option.TLSCertificateFile = *sinkPulsar.TLSCertificateFile
			option.TLSKeyFilePath = *sinkPulsar.TLSKeyFilePath
			log.Info("pulsar tls certificate file and tls key file path is set, tls ")
		}
	}

	pulsarClient, err := pulsar.NewClient(option)
	if err != nil {
		log.Error("cannot connect to pulsar", zap.Error(err))
		return nil, err
	}
	return pulsarClient, nil
}

// setupAuthentication sets up authentication for pulsar client
// returns true if authentication is tls authentication , and the authentication object
func setupAuthentication(config *config.PulsarConfig) (bool, pulsar.Authentication, error) {
	if config.AuthenticationToken != nil {
		log.Info("pulsar token authentication is set, use toke authentication")
		return false, pulsar.NewAuthenticationToken(*config.AuthenticationToken), nil
	}
	if config.TokenFromFile != nil {
		log.Info("pulsar token from file authentication is set, use toke authentication")
		res := pulsar.NewAuthenticationTokenFromFile(*config.TokenFromFile)
		return false, res, nil
	}
	if config.BasicUserName != nil && config.BasicPassword != nil {
		log.Info("pulsar basic authentication is set, use basic authentication")
		res, err := pulsar.NewAuthenticationBasic(*config.BasicUserName, *config.BasicPassword)
		return false, res, err
	}
	if config.OAuth2 != nil {
		oauth2 := map[string]string{
			auth.ConfigParamIssuerURL: config.OAuth2.OAuth2IssuerURL,
			auth.ConfigParamAudience:  config.OAuth2.OAuth2Audience,
			auth.ConfigParamScope:     config.OAuth2.OAuth2Scope,
			auth.ConfigParamKeyFile:   config.OAuth2.OAuth2PrivateKey,
			auth.ConfigParamClientID:  config.OAuth2.OAuth2ClientID,
			auth.ConfigParamType:      auth.ConfigParamTypeClientCredentials,
		}
		log.Info("pulsar oauth2 authentication is set, use oauth2 authentication")
		return false, pulsar.NewAuthenticationOAuth2(oauth2), nil
	}
	if config.AuthTLSCertificatePath != nil && config.AuthTLSPrivateKeyPath != nil {
		log.Info("pulsar mTLS authentication is set, use mTLS authentication")
		return true, pulsar.NewAuthenticationTLS(*config.AuthTLSCertificatePath, *config.AuthTLSPrivateKeyPath), nil
	}
	log.Info("No authentication configured for pulsar client")
	return false, nil, nil
}

// NewMockCreatorFactory returns a factory implemented based on kafka-go
func NewMockCreatorFactory(config *config.PulsarConfig, changefeedID model.ChangeFeedID,
	sinkConfig *config.SinkConfig,
) (pulsar.Client, error) {
	log.Info("mock pulsar client factory created", zap.Any("changfeedID", changefeedID))
	return nil, nil
}
