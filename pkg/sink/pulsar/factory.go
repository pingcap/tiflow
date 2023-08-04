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
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"go.uber.org/zap"
)

// FactoryCreator defines the type of factory creator.
type FactoryCreator func(config *Config, changefeedID model.ChangeFeedID, sinkConfig *config.SinkConfig) (pulsar.Client, error)

// NewCreatorFactory returns a factory implemented based on kafka-go
func NewCreatorFactory(config *Config, changefeedID model.ChangeFeedID, sinkConfig *config.SinkConfig) (pulsar.Client, error) {
	co := pulsar.ClientOptions{
		URL: config.URL,
		CustomMetricsLabels: map[string]string{
			"changefeed": changefeedID.ID,
			"namespace":  changefeedID.Namespace,
		},
		ConnectionTimeout: config.ConnectionTimeout,
		OperationTimeout:  config.OperationTimeout,
	}
	var err error

	co.Authentication, err = setupAuthentication(config)
	if err != nil {
		log.Error("setup pulsar authentication fail", zap.Error(err))
		return nil, err
	}

	// pulsar TLS config
	if sinkConfig.PulsarConfig != nil {
		sinkPulsar := sinkConfig.PulsarConfig
		if sinkPulsar.TLSCertificateFile != nil && sinkPulsar.TLSKeyFilePath != nil &&
			sinkPulsar.TLSTrustCertsFilePath != nil {
			co.TLSCertificateFile = *sinkPulsar.TLSCertificateFile
			co.TLSKeyFilePath = *sinkPulsar.TLSKeyFilePath
			co.TLSTrustCertsFilePath = *sinkPulsar.TLSTrustCertsFilePath
		}
	}

	pulsarClient, err := pulsar.NewClient(co)
	if err != nil {
		log.Error("cannot connect to pulsar", zap.Error(err))
		return nil, err
	}
	return pulsarClient, nil
}

// setupAuthentication sets up authentication for pulsar client
func setupAuthentication(config *Config) (pulsar.Authentication, error) {
	if len(config.AuthenticationToken) > 0 {
		return pulsar.NewAuthenticationToken(config.AuthenticationToken), nil
	} else if len(config.TokenFromFile) > 0 {
		return pulsar.NewAuthenticationTokenFromFile(config.TokenFromFile), nil
	} else if len(config.BasicUserName) > 0 && len(config.BasicPassword) > 0 {
		return pulsar.NewAuthenticationBasic(config.BasicUserName, config.BasicPassword)
	} else if len(config.OAuth2) >= 5 {
		return pulsar.NewAuthenticationOAuth2(config.OAuth2), nil
	} else if len(config.AuthTLSCertificatePath) > 0 && len(config.AuthTLSPrivateKeyPath) > 0 {
		return pulsar.NewAuthenticationTLS(config.AuthTLSCertificatePath, config.AuthTLSPrivateKeyPath), nil
	}
	return nil, fmt.Errorf("no authentication method found")
}

// NewMockCreatorFactory returns a factory implemented based on kafka-go
func NewMockCreatorFactory(config *Config, changefeedID model.ChangeFeedID,
	sinkConfig *config.SinkConfig,
) (pulsar.Client, error) {
	log.Info("mock pulsar client factory created", zap.Any("changfeedID", changefeedID))
	return nil, nil
}
