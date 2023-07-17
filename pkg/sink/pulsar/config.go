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
	PulsarVersion   = "pulsar-version"
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

	// SendTimeout producer max send message timeout  (default: 1000ms)
	SendTimeout = "send-timeout"

	// ProducerMode single or batch send message(s)
	ProducerMode = "producer-mode"

	MessageKey = "message-key"

	BasicUserName = "basic-user-name"

	BasicPassword = "basic-password"

	TokenFromFile = "token-from-file"

	DebugMode = "debug-mode"

	Protocol = "protocol"
)

// sink config default Value
const (
	defaultConnectionTimeout = 5 * time.Second

	defaultOperationTimeout = 30 * time.Second

	defaultBatchingMaxSize = uint(1000)

	defaultBatchingMaxDelay = 10 * time.Millisecond

	// defaultSendTimeout 1000ms
	defaultSendTimeout = 1000 * time.Millisecond

	// defaultProducerModeSingle batch send message(s)
	defaultProducerModeBatch = "batch"
)

type PulsarConfig struct {
	PulsarVersion string
	// pulsar server message limited 5MB default
	MaxMessageBytes int
	//Compression     string

	// Configure the service URL for the Pulsar service.
	// This parameter is required
	URL string

	AuthenticationToken string

	ConnectionTimeout time.Duration

	OperationTimeout time.Duration

	// producer config
	BatchingMaxMessages uint

	BatchingMaxPublishDelay time.Duration

	// default: 1000ms
	SendTimeout time.Duration

	// ProducerMode single or batch send message(s)
	ProducerMode string

	// messageKey
	MessageKey string

	// TokenFromFile token file path
	TokenFromFile string

	// BasicUserName
	BasicUserName string
	// BasicPassword
	BasicPassword string

	// DebugMode
	DebugMode bool

	Protocol config.Protocol

	// parse the sinkURI
	u *url.URL

	// pulsar client compression
	CompressionType pulsar.CompressionType
}

// GetBrokerURL get broker url
func (c *PulsarConfig) GetBrokerURL() string {
	return c.URL
}

// GetSinkURI
func (c *PulsarConfig) GetSinkURI() *url.URL {
	return c.u
}

// Apply apply
func (c *PulsarConfig) Apply(sinkURI *url.URL) error {
	params := sinkURI.Query()

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
		//c.Compression = s
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

	c.URL = sinkURI.Scheme + "://" + sinkURI.Host
	if len(c.URL) == 0 {
		return fmt.Errorf("URL is empty")
	}
	c.u = sinkURI

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
		c.SendTimeout = time.Millisecond * time.Duration(a)
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
		BatchingMaxPublishDelay: defaultBatchingMaxDelay,
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
