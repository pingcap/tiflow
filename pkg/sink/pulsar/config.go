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
	"github.com/apache/pulsar-client-go/pulsar/auth"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// sink config Key
const (
	PulsarVersion = "pulsar-version"

	MaxMessageBytes = "max-message-bytes"

	// Compression 	support LZ4 ZLib ZSTD
	Compression = "compression"

	// AuthenticationToken Authentication token
	AuthenticationToken = "authentication-token"

	// ConnectionTimeout Timeout for the establishment of a TCP connection (default: 5 seconds)
	ConnectionTimeout = "connection-timeout"

	// OperationTimeout Set the operation timeout (default: 30 seconds)
	// Producer-create, subscribe and unsubscribe operations will be retried until this interval, after which the
	// operation will be marked as failed
	OperationTimeout = "operation-timeout"

	// BatchingMaxMessages specifies the maximum number of messages permitted in a batch. (default: 1000)
	// If set to a value greater than 1, messages will be queued until this threshold is reached or
	// BatchingMaxSize (see below) has been reached or the batch interval has elapsed.
	BatchingMaxMessages = "batching-max-messages"

	// BatchingMaxPublishDelay specifies the time period within which the messages sent will be batched (default: 10ms)
	// if batch messages are enabled. If set to a non zero value, messages will be queued until this time
	// interval or until
	BatchingMaxPublishDelay = "batching-max-publish-delay"

	// SendTimeout producer max send message timeout  (default: 30000ms)
	SendTimeout = "send-timeout"

	// BasicUserName Account name for pulsar basic authentication (the second priority authentication method)
	BasicUserName = "basic-user-name"

	BasicPassword = "basic-password"

	// TokenFromFile Authentication from the file token,
	// the path name of the file (the third priority authentication method)
	TokenFromFile = "token-from-file"

	// Protocol The message protocol type input to pulsar, pulsar currently supports canal-json, canal, maxwell
	Protocol = "protocol"

	// OAuth2IssuerURL  the URL of the authorization server.
	OAuth2IssuerURL = "oauth2-issuer-url"
	// OAuth2Audience  the URL of the resource server.
	OAuth2Audience = "oauth2-audience"
	// OAuth2PrivateKey the private key used to sign the server.
	OAuth2PrivateKey = "oauth2-private-key"
	// OAuth2ClientID  the client ID of the application.
	OAuth2ClientID = "oauth2-client-id"
	// OAuth2Type  the type of the OAuth2 .
	OAuth2Type = "oauth2-type"
	// OAuth2TypeClientCredentials  client_credentials
	OAuth2TypeClientCredentials = "oauth2-client-credentials"
	// OAuth2Scope scope
	OAuth2Scope = "auth2-scope"
)

// sink config default Value
const (
	defaultConnectionTimeout = 5 * time.Second

	defaultOperationTimeout = 30 * time.Second

	defaultBatchingMaxSize = uint(1000)

	defaultBatchingMaxPublishDelay = 10 * time.Millisecond

	// defaultSendTimeout 30s
	defaultSendTimeout = 30 * time.Second

	// defaultProducerModeSingle batch send message(s)
	defaultProducerModeBatch = "batch"
)

// Config pulsar sink config
type Config struct {
	PulsarVersion string
	// MaxMessageBytes pulsar server message limited 5MB default,
	// this default value is 0 !
	// but it can not be set by client. if you set it,
	// we will check message if greater than MaxMessageBytes before produce at client.
	MaxMessageBytes int

	// Configure the service URL for the Pulsar service.
	// This parameter is required
	URL string

	// pulsar client compression
	CompressionType pulsar.CompressionType

	// AuthenticationToken the token for the Pulsar server
	AuthenticationToken string

	// ConnectionTimeout Timeout for the establishment of a TCP connection (default: 5 seconds)
	ConnectionTimeout time.Duration

	// Set the operation timeout (default: 30 seconds)
	// Producer-create, subscribe and unsubscribe operations will be retried until this interval, after which the
	// operation will be marked as failed
	OperationTimeout time.Duration

	// BatchingMaxMessages specifies the maximum number of messages permitted in a batch. (default: 1000)
	BatchingMaxMessages uint

	// BatchingMaxPublishDelay specifies the time period within which the messages sent will be batched (default: 10ms)
	// if batch messages are enabled. If set to a non zero value, messages will be queued until this time
	// interval or until
	BatchingMaxPublishDelay time.Duration

	// SendTimeout specifies the timeout for a message that has not been acknowledged by the server since sent.
	// Send and SendAsync returns an error after timeout.
	// default: 30s
	SendTimeout time.Duration

	// ProducerMode batch send message(s)
	ProducerMode string

	// TokenFromFile Authentication from the file token,
	// the path name of the file (the third priority authentication method)
	TokenFromFile string

	// BasicUserName Account name for pulsar basic authentication (the second priority authentication method)
	BasicUserName string
	// BasicPassword with account
	BasicPassword string

	// TLSCertificatePath  create new pulsar authentication provider with specified TLS certificate and private key
	TLSCertificatePath string
	// TLSPrivateKeyPath private key
	TLSPrivateKeyPath string

	// Oauth2 include  oauth2-issuer-url oauth2-audience oauth2-private-key oauth2-client-id
	// and 'type' always is 'client_credentials'
	OAuth2 map[string]string

	// DebugMode
	DebugMode bool

	// Protocol The message protocol type input to pulsar, pulsar currently supports canal-json, canal, maxwell
	Protocol config.Protocol

	// parse the sinkURI
	u *url.URL
}

// GetBrokerURL get broker url
func (c *Config) GetBrokerURL() string {
	return c.URL
}

// GetSinkURI get sink uri
func (c *Config) GetSinkURI() *url.URL {
	return c.u
}

func (c *Config) checkSinkURI(sinkURI *url.URL) error {
	if sinkURI.Scheme == "" {
		return fmt.Errorf("scheme is empty")
	}
	if sinkURI.Host == "" {
		return fmt.Errorf("host is empty")
	}
	if sinkURI.Path == "" {
		return fmt.Errorf("path is empty")
	}
	return nil
}

func (c *Config) applyOAuth(params url.Values) {
	// Go client use Oauth2 authentication
	// https://pulsar.apache.org/docs/2.10.x/security-oauth2/#authentication-types
	// pulsar client now support type as client_credentials only

	s := params.Get(OAuth2IssuerURL)
	if len(s) > 0 {
		c.OAuth2[auth.ConfigParamIssuerURL] = s
	}
	s = params.Get(OAuth2Audience)
	if len(s) > 0 {
		c.OAuth2[auth.ConfigParamAudience] = s
	}
	s = params.Get(OAuth2Scope)
	if len(s) > 0 {
		c.OAuth2[auth.ConfigParamScope] = s
	}
	s = params.Get(OAuth2PrivateKey)
	if len(s) > 0 {
		c.OAuth2[auth.ConfigParamKeyFile] = s
	}
	s = params.Get(OAuth2ClientID)
	if len(s) > 0 {
		c.OAuth2[auth.ConfigParamClientID] = s
	}
	if len(c.OAuth2) >= 4 {
		c.OAuth2[auth.ConfigParamType] = auth.ConfigParamTypeClientCredentials
	} else {
		c.OAuth2 = make(map[string]string)
	}
}

// Apply apply
func (c *Config) Apply(sinkURI *url.URL) error {
	err := c.checkSinkURI(sinkURI)
	if err != nil {
		return err
	}

	params := sinkURI.Query()

	c.URL = sinkURI.Scheme + "://" + sinkURI.Host
	if len(c.URL) == 0 {
		return fmt.Errorf("URL is empty")
	}
	c.u = sinkURI

	s := params.Get(PulsarVersion)
	if s != "" {
		c.PulsarVersion = s
	}

	s = params.Get(MaxMessageBytes)
	if s != "" {
		a, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		c.MaxMessageBytes = a
	}

	s = params.Get(Compression)
	if s != "" {
		switch strings.ToLower(s) {
		case "lz4":
			c.CompressionType = pulsar.LZ4
		case "zlib":
			c.CompressionType = pulsar.ZLib
		case "zstd":
			c.CompressionType = pulsar.ZSTD
		}
	}

	s = params.Get(AuthenticationToken)
	if len(s) > 0 {
		c.AuthenticationToken = s
	}

	s = params.Get(ConnectionTimeout)
	if len(s) > 0 {
		a, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		c.ConnectionTimeout = time.Second * time.Duration(a)
	}

	s = params.Get(OperationTimeout)
	if len(s) > 0 {
		a, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		c.OperationTimeout = time.Second * time.Duration(a)
	}

	s = params.Get(BatchingMaxMessages)
	if len(s) > 0 {
		a, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		c.BatchingMaxMessages = uint(a)
	}

	s = params.Get(BatchingMaxPublishDelay)
	if len(s) > 0 {
		a, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		c.BatchingMaxPublishDelay = time.Millisecond * time.Duration(a)
	}

	s = params.Get(SendTimeout)
	if len(s) > 0 {
		a, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		c.SendTimeout = time.Second * time.Duration(a)
	}

	s = params.Get(TokenFromFile)
	if len(s) > 0 {
		c.TokenFromFile = s
	}

	s = params.Get(BasicUserName)
	if len(s) > 0 {
		c.BasicUserName = s
	}

	s = params.Get(BasicPassword)
	if len(s) > 0 {
		c.BasicPassword = s
	}

	c.applyOAuth(params)

	s = params.Get(Protocol)
	if len(s) > 0 {
		protocol, err := config.ParseSinkProtocolFromString(s)
		if err != nil {
			return err
		}
		switch protocol {
		case config.ProtocolCanalJSON, config.ProtocolCanal, config.ProtocolMaxwell:
			c.Protocol = protocol
		default:
			return cerror.ErrSinkUnknownProtocol.GenWithStackByArgs(protocol)
		}
	}

	log.L().Debug("pulsar config apply", zap.Any("config", c),
		zap.Any("params", params))

	return nil
}

// NewPulsarConfig new pulsar config
func NewPulsarConfig(sinkURI *url.URL) (*Config, error) {
	c := &Config{
		u:                       sinkURI,
		ConnectionTimeout:       defaultConnectionTimeout,
		OperationTimeout:        defaultOperationTimeout,
		BatchingMaxMessages:     defaultBatchingMaxSize,
		BatchingMaxPublishDelay: defaultBatchingMaxPublishDelay,
		// from pkg/config.go
		MaxMessageBytes: config.DefaultMaxMessageBytes,
		SendTimeout:     defaultSendTimeout,
		ProducerMode:    defaultProducerModeBatch,
	}
	err := c.Apply(sinkURI)
	if err != nil {
		log.L().Error("NewPulsarConfig failed", zap.Error(err))
		return nil, err
	}
	return c, nil
}

// GetDefaultTopicName get default topic name
func (c *Config) GetDefaultTopicName() string {
	topicName := c.u.Path
	return topicName[1:]
}
