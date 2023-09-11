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
		Logger:            NewPulsarLogger(),
	}
	var err error

	option.Authentication, err = setupAuthentication(config)
	if err != nil {
		log.Error("setup pulsar authentication fail", zap.Error(err))
		return nil, err
	}

	// pulsar TLS config
	if sinkConfig.PulsarConfig != nil {
		sinkPulsar := sinkConfig.PulsarConfig
		if sinkPulsar.TLSCertificateFile != nil && sinkPulsar.TLSKeyFilePath != nil &&
			sinkPulsar.TLSTrustCertsFilePath != nil {
			option.TLSCertificateFile = *sinkPulsar.TLSCertificateFile
			option.TLSKeyFilePath = *sinkPulsar.TLSKeyFilePath
			option.TLSTrustCertsFilePath = *sinkPulsar.TLSTrustCertsFilePath
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
func setupAuthentication(config *config.PulsarConfig) (pulsar.Authentication, error) {
	if config.AuthenticationToken != nil {
		return pulsar.NewAuthenticationToken(*config.AuthenticationToken), nil
	}
	if config.TokenFromFile != nil {
		return pulsar.NewAuthenticationTokenFromFile(*config.TokenFromFile), nil
	}
	if config.BasicUserName != nil && config.BasicPassword != nil {
		return pulsar.NewAuthenticationBasic(*config.BasicUserName, *config.BasicPassword)
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
		return pulsar.NewAuthenticationOAuth2(oauth2), nil
	}
	if config.AuthTLSCertificatePath != nil && config.AuthTLSPrivateKeyPath != nil {
		return pulsar.NewAuthenticationTLS(*config.AuthTLSCertificatePath, *config.AuthTLSPrivateKeyPath), nil
	}
	log.Info("No authentication configured for pulsar client")
	return nil, nil
}

// NewMockCreatorFactory returns a factory implemented based on kafka-go
func NewMockCreatorFactory(config *config.PulsarConfig, changefeedID model.ChangeFeedID,
	sinkConfig *config.SinkConfig,
) (pulsar.Client, error) {
	log.Info("mock pulsar client factory created", zap.Any("changfeedID", changefeedID))
	return nil, nil
}
