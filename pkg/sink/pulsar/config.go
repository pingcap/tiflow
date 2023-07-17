package pulsar

import (
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"net/url"
	"strconv"
	"strings"
	"time"
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

	// MessageKey Option.All Keys filled in Pulsar when sending events are used for routing in Pulsar. If it is empty, it will be randomly distributed in each partition.
	//For one changefeed task.
	MessageKey = "message-key"

	// BasicUserName Account name for pulsar basic authentication (the second priority authentication method)
	BasicUserName = "basic-user-name"

	BasicPassword = "basic-password"

	// TokenFromFile Authentication from the file token, the path name of the file (the third priority authentication method)
	TokenFromFile = "token-from-file"

	// Protocol The message protocol type input to pulsar, pulsar currently supports canal-json, canal, maxwell
	Protocol = "protocol"
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

type PulsarConfig struct {
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

	// MessageKey Option.All Keys filled in Pulsar when sending events are used for routing in Pulsar. If it is empty, it will be randomly distributed in each partition.
	//For one changefeed task.
	MessageKey string

	// TokenFromFile Authentication from the file token, the path name of the file (the third priority authentication method)
	TokenFromFile string

	// BasicUserName Account name for pulsar basic authentication (the second priority authentication method)
	BasicUserName string
	// BasicPassword with account
	BasicPassword string

	// DebugMode
	DebugMode bool

	// Protocol The message protocol type input to pulsar, pulsar currently supports canal-json, canal, maxwell
	Protocol config.Protocol

	// parse the sinkURI
	u *url.URL
}

// GetBrokerURL get broker url
func (c *PulsarConfig) GetBrokerURL() string {
	return c.URL
}

// GetSinkURI
func (c *PulsarConfig) GetSinkURI() *url.URL {
	return c.u
}

func (c *PulsarConfig) checkSinkURI(sinkURI *url.URL) error {
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

// Apply apply
func (c *PulsarConfig) Apply(sinkURI *url.URL) error {
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

	s = params.Get(MessageKey)
	if len(s) > 0 {
		c.MessageKey = s
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

func NewPulsarConfig(sinkURI *url.URL) (*PulsarConfig, error) {
	c := &PulsarConfig{
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

func (c *PulsarConfig) GetDefaultTopicName() string {
	topicName := c.u.Path
	return topicName[1:]
}
