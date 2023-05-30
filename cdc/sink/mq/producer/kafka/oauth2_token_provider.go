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
	"net/url"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// tsokenProvider is a user-defined callback for generating
// access tokens for SASL/OAUTHBEARER auth.
type tokenProvider struct {
	tokenSource oauth2.TokenSource
}

var _ sarama.AccessTokenProvider = (*tokenProvider)(nil)

// Token implements the sarama.AccessTokenProvider interface.
// Token returns an access token. The implementation should ensure token
// reuse so that multiple calls at connect time do not create multiple
// tokens. The implementation should also periodically refresh the token in
// order to guarantee that each call returns an unexpired token.  This
// method should not block indefinitely--a timeout error should be returned
// after a short period of inactivity so that the broker connection logic
// can log debugging information and retry.
func (t *tokenProvider) Token() (*sarama.AccessToken, error) {
	token, err := t.tokenSource.Token()
	if err != nil {
		// Errors will result in Sarama retrying the broker connection and logging
		// the transient error, with a Broker connection error surfacing after retry
		// attempts have been exhausted.
		return nil, err
	}

	return &sarama.AccessToken{Token: token.AccessToken}, nil
}

func newTokenProvider(ctx context.Context,
	kafkaConfig *Config,
) (sarama.AccessTokenProvider, error) {
	// grant_type is by default going to be set to 'client_credentials' by the
	// clientcredentials library as defined by the spec, however non-compliant
	// auth server implementations may want a custom type
	var endpointParams url.Values
	if kafkaConfig.SASL.OAuth2.GrantType != "" {
		if endpointParams == nil {
			endpointParams = url.Values{}
		}
		endpointParams.Set("grant_type", kafkaConfig.SASL.OAuth2.GrantType)
	}

	// audience is an optional parameter that can be used to specify the
	// intended audience of the token.
	if kafkaConfig.SASL.OAuth2.Audience != "" {
		if endpointParams == nil {
			endpointParams = url.Values{}
		}
		endpointParams.Set("audience", kafkaConfig.SASL.OAuth2.Audience)
	}

	tokenURL, err := url.Parse(kafkaConfig.SASL.OAuth2.TokenURL)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cfg := clientcredentials.Config{
		ClientID:       kafkaConfig.SASL.OAuth2.ClientID,
		ClientSecret:   kafkaConfig.SASL.OAuth2.ClientSecret,
		TokenURL:       tokenURL.String(),
		EndpointParams: endpointParams,
		Scopes:         kafkaConfig.SASL.OAuth2.Scopes,
	}
	return &tokenProvider{
		tokenSource: cfg.TokenSource(ctx),
	}, nil
}
