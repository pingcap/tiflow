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

package v2

import (
	"context"
	"crypto/tls"
	"strings"
	"time"

	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	pkafka "github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"go.uber.org/zap"
)

type factory struct {
	// transport is used to contact kafka cluster and also maintain the `metadata cache`
	// it's shared by the admin client and producers to keep the cache the same to make
	// sure that the newly created topics can be found by the both.
	transport    *kafka.Transport
	changefeedID model.ChangeFeedID
	options      *pkafka.Options

	writer *kafka.Writer
}

// NewFactory returns a factory implemented based on kafka-go
func NewFactory(
	options *pkafka.Options,
	changefeedID model.ChangeFeedID,
) (pkafka.Factory, error) {
	transport, err := newTransport(options)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &factory{
		transport:    transport,
		changefeedID: changefeedID,
		options:      options,
		writer:       &kafka.Writer{},
	}, nil
}

func newClient(brokerEndpoints []string, transport *kafka.Transport) *kafka.Client {
	return &kafka.Client{
		Addr: kafka.TCP(brokerEndpoints...),
		// todo: make this configurable
		Timeout:   10 * time.Second,
		Transport: transport,
	}
}

func newTransport(o *pkafka.Options) (*kafka.Transport, error) {
	mechanism, err := completeSASLConfig(o)
	if err != nil {
		return nil, err
	}
	tlsConfig, err := completeSSLConfig(o)
	if err != nil {
		return nil, err
	}
	return &kafka.Transport{
		SASL:        mechanism,
		TLS:         tlsConfig,
		DialTimeout: o.DialTimeout,
		ClientID:    o.ClientID,
	}, nil
}

func completeSSLConfig(options *pkafka.Options) (*tls.Config, error) {
	if options.EnableTLS {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
			NextProtos: []string{"h2", "http/1.1"},
		}

		// for SSL encryption with self-signed CA certificate, we reassign the
		// config.Net.TLS.Config using the relevant credential files.
		if options.Credential != nil && options.Credential.IsTLSEnabled() {
			tlsConfig, err := options.Credential.ToTLSConfig()
			return tlsConfig, errors.Trace(err)
		}

		tlsConfig.InsecureSkipVerify = options.InsecureSkipVerify
		return tlsConfig, nil
	}
	return nil, nil
}

func completeSASLConfig(o *pkafka.Options) (sasl.Mechanism, error) {
	if o.SASL != nil && o.SASL.SASLMechanism != "" {
		switch o.SASL.SASLMechanism {
		case pkafka.SASLTypeSCRAMSHA256, pkafka.SASLTypeSCRAMSHA512, pkafka.SASLTypePlaintext:
			if strings.EqualFold(string(o.SASL.SASLMechanism), pkafka.SASLTypeSCRAMSHA256) {
				mechanism, err := scram.Mechanism(scram.SHA256,
					o.SASL.SASLUser, o.SASL.SASLPassword)
				return mechanism, errors.Trace(err)
			} else if strings.EqualFold(string(o.SASL.SASLMechanism), pkafka.SASLTypeSCRAMSHA512) {
				mechanism, err := scram.Mechanism(scram.SHA512,
					o.SASL.SASLUser, o.SASL.SASLPassword)
				return mechanism, errors.Trace(err)
			} else {
				return plain.Mechanism{
					Username: o.SASL.SASLUser,
					Password: o.SASL.SASLPassword,
				}, nil
			}
		case pkafka.SASLTypeGSSAPI:
			cfg, err := config.Load(o.SASL.GSSAPI.KerberosConfigPath)
			if err != nil {
				return nil, errors.Trace(err)
			}
			var clnt *client.Client
			switch o.SASL.GSSAPI.AuthType {
			case security.UserAuth:
				clnt = client.NewWithPassword(o.SASL.GSSAPI.Username, o.SASL.GSSAPI.Realm,
					o.SASL.GSSAPI.Password, cfg,
					client.DisablePAFXFAST(o.SASL.GSSAPI.DisablePAFXFAST))
			case security.KeyTabAuth:
				ktab, err := keytab.Load(o.SASL.GSSAPI.KeyTabPath)
				if err != nil {
					return nil, errors.Trace(err)
				}
				clnt = client.NewWithKeytab(o.SASL.GSSAPI.Username, o.SASL.GSSAPI.Realm, ktab, cfg,
					client.DisablePAFXFAST(o.SASL.GSSAPI.DisablePAFXFAST))
			}
			err = clnt.Login()
			if err != nil {
				return nil, errors.Trace(err)
			}
			return Gokrb5v8(&gokrb5v8ClientImpl{clnt},
				o.SASL.GSSAPI.ServiceName), nil

		case pkafka.SASLTypeOAuth:
			return nil, errors.ErrKafkaInvalidConfig.GenWithStack(
				"OAuth is not yet supported in Kafka sink v2")
		}
	}
	return nil, nil
}

func (f *factory) newWriter(async bool) *kafka.Writer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(f.options.BrokerEndpoints...),
		Balancer:     newManualPartitioner(),
		Transport:    f.transport,
		ReadTimeout:  f.options.ReadTimeout,
		WriteTimeout: f.options.WriteTimeout,
		// For kafka cluster with a bad network condition,
		// do not waste too much time to prevent long time blocking.
		MaxAttempts:     2,
		WriteBackoffMin: 10 * time.Millisecond,
		RequiredAcks:    kafka.RequiredAcks(f.options.RequiredAcks),
		BatchBytes:      int64(f.options.MaxMessageBytes),
		Async:           async,
	}
	f.writer = w
	compression := strings.ToLower(strings.TrimSpace(f.options.Compression))
	switch compression {
	case "none":
	case "gzip":
		w.Compression = kafka.Gzip
	case "snappy":
		w.Compression = kafka.Snappy
	case "lz4":
		w.Compression = kafka.Lz4
	case "zstd":
		w.Compression = kafka.Zstd
	default:
		log.Warn("Unsupported compression algorithm",
			zap.String("namespace", f.changefeedID.Namespace),
			zap.String("changefeed", f.changefeedID.ID),
			zap.String("compression", f.options.Compression))
	}
	log.Info("Kafka producer uses "+f.options.Compression+" compression algorithm",
		zap.String("namespace", f.changefeedID.Namespace),
		zap.String("changefeed", f.changefeedID.ID))
	return w
}

func (f *factory) AdminClient(_ context.Context) (pkafka.ClusterAdminClient, error) {
	return newClusterAdminClient(f.options.BrokerEndpoints, f.transport, f.changefeedID), nil
}

// SyncProducer creates a sync producer to writer message to kafka
func (f *factory) SyncProducer(_ context.Context) (pkafka.SyncProducer, error) {
	w := f.newWriter(false)
	// set batch size to 1 to make sure the message is sent immediately
	w.BatchTimeout = time.Millisecond
	w.BatchSize = 1
	return &syncWriter{
		w:            w,
		changefeedID: f.changefeedID,
	}, nil
}

// AsyncProducer creates an async producer to writer message to kafka
func (f *factory) AsyncProducer(
	ctx context.Context,
	failpointCh chan error,
) (pkafka.AsyncProducer, error) {
	w := f.newWriter(true)
	// assume each message is 1KB,
	// and set batch timeout to 5ms to avoid waste too much time on waiting for messages.
	w.BatchTimeout = 5 * time.Millisecond
	w.BatchSize = int(w.BatchBytes / 1024)
	aw := &asyncWriter{
		w:            w,
		changefeedID: f.changefeedID,
		failpointCh:  failpointCh,
		errorsChan:   make(chan error, 1),
	}

	w.Completion = func(messages []kafka.Message, err error) {
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case aw.errorsChan <- err:
			default:
				log.Warn("async writer report error failed, since the err channel is full",
					zap.String("namespace", aw.changefeedID.Namespace),
					zap.String("changefeed", aw.changefeedID.ID),
					zap.Error(err))
			}
			return
		}

		for _, msg := range messages {
			callback := msg.WriterData.(func())
			if callback != nil {
				callback()
			}
		}
	}

	return aw, nil
}

// MetricsCollector returns the kafka metrics collector
func (f *factory) MetricsCollector(
	role util.Role,
	adminClient pkafka.ClusterAdminClient,
) pkafka.MetricsCollector {
	return NewMetricsCollector(f.changefeedID, role, f.writer)
}

type syncWriter struct {
	changefeedID model.ChangeFeedID
	w            Writer
}

func (s *syncWriter) SendMessage(
	ctx context.Context,
	topic string, partitionNum int32,
	message *common.Message,
) error {
	return s.w.WriteMessages(ctx, kafka.Message{
		Topic:     topic,
		Partition: int(partitionNum),
		Key:       message.Key,
		Value:     message.Value,
	})
}

// SendMessages produces a given set of messages, and returns only when all
// messages in the set have either succeeded or failed. Note that messages
// can succeed and fail individually; if some succeed and some fail,
// SendMessages will return an error.
func (s *syncWriter) SendMessages(ctx context.Context, topic string, partitionNum int32, message *common.Message) error {
	msgs := make([]kafka.Message, int(partitionNum))
	for i := 0; i < int(partitionNum); i++ {
		msgs[i] = kafka.Message{
			Topic:     topic,
			Key:       message.Key,
			Value:     message.Value,
			Partition: i,
		}
	}
	return s.w.WriteMessages(ctx, msgs...)
}

// Close shuts down the producer; you must call this function before a producer
// object passes out of scope, as it may otherwise leak memory.
// You must call this before calling Close on the underlying client.
func (s *syncWriter) Close() {
	log.Info("kafka sync producer start closing",
		zap.String("namespace", s.changefeedID.Namespace),
		zap.String("changefeed", s.changefeedID.ID))
	start := time.Now()
	if err := s.w.Close(); err != nil {
		log.Warn("Close kafka sync producer failed",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err))
	} else {
		log.Info("Close kafka sync producer success",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID),
			zap.Duration("duration", time.Since(start)))
	}
}

type asyncWriter struct {
	w            Writer
	changefeedID model.ChangeFeedID
	failpointCh  chan error
	errorsChan   chan error
}

// Close shuts down the producer and waits for any buffered messages to be
// flushed. You must call this function before a producer object passes out of
// scope, as it may otherwise leak memory. You must call this before process
// shutting down, or you may lose messages. You must call this before calling
// Close on the underlying client.
func (a *asyncWriter) Close() {
	log.Info("kafka async producer start closing",
		zap.String("namespace", a.changefeedID.Namespace),
		zap.String("changefeed", a.changefeedID.ID))
	go func() {
		start := time.Now()
		if err := a.w.Close(); err != nil {
			log.Warn("Close kafka async producer failed",
				zap.String("namespace", a.changefeedID.Namespace),
				zap.String("changefeed", a.changefeedID.ID),
				zap.Duration("duration", time.Since(start)),
				zap.Error(err))
		} else {
			log.Info("Close kafka async producer success",
				zap.String("namespace", a.changefeedID.Namespace),
				zap.String("changefeed", a.changefeedID.ID),
				zap.Duration("duration", time.Since(start)))
		}
	}()
}

// AsyncSend is the input channel for the user to write messages to that they
// wish to send.
func (a *asyncWriter) AsyncSend(ctx context.Context, topic string, partition int32, message *common.Message) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}
	return a.w.WriteMessages(ctx, kafka.Message{
		Topic:      topic,
		Partition:  int(partition),
		Key:        message.Key,
		Value:      message.Value,
		WriterData: message.Callback,
	})
}

// AsyncRunCallback process the messages that has sent to kafka,
// and run tha attached callback. the caller should call this
// method in a background goroutine
func (a *asyncWriter) AsyncRunCallback(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case err := <-a.failpointCh:
		log.Warn("Receive from failpoint chan in kafka producer",
			zap.String("namespace", a.changefeedID.Namespace),
			zap.String("changefeed", a.changefeedID.ID),
			zap.Error(err))
		return errors.Trace(err)
	case err := <-a.errorsChan:
		// We should not wrap a nil pointer if the pointer
		// is of a subtype of `error` because Go would store the type info
		// and the resulted `error` variable would not be nil,
		// which will cause the pkg/error library to malfunction.
		// See: https://go.dev/doc/faq#nil_error
		if err == nil {
			return nil
		}
		return errors.WrapError(errors.ErrKafkaAsyncSendMessage, err)
	}
}
