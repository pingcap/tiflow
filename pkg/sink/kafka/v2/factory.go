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

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	pkafka "github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/rcrowley/go-metrics"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"go.uber.org/zap"
)

type Factory struct {
	transport *kafka.Transport
	client    *kafka.Client
	options   *pkafka.Options
}

// NewFactory constructs a Client with kafka go.
func NewFactory(ctx context.Context, options *pkafka.Options) (pkafka.Factory, error) {
	captureAddr := contextutil.CaptureAddrFromCtx(ctx)
	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)
	var role string
	if contextutil.IsOwnerFromCtx(ctx) {
		role = util.RoleOwner.String()
	} else {
		role = util.RoleProcessor.String()
	}
	clientID, err := pkafka.NewKafkaClientID(role, captureAddr, changefeedID, options.ClientID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	mechanism, err := completeSASLConfig(options)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tlsConfig, err := completeSSLConfig(options)
	if err != nil {
		return nil, errors.Trace(err)
	}
	transport := &kafka.Transport{
		SASL:        mechanism,
		ClientID:    clientID,
		TLS:         tlsConfig,
		IdleTimeout: options.DialTimeout,
	}
	client := &kafka.Client{
		Addr: kafka.TCP(options.BrokerEndpoints...),
		// todo: make this configurable
		Timeout:   10 * time.Second,
		Transport: transport,
	}
	return &Factory{
		transport: transport,
		client:    client,
		options:   options,
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
			// todo: support gss api
		}
	}
	return nil, nil
}

func (k *Factory) createWriter() *kafka.Writer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(k.options.BrokerEndpoints...),
		Balancer:     newManualPartitioner(),
		Transport:    k.transport,
		ReadTimeout:  k.options.ReadTimeout,
		WriteTimeout: k.options.WriteTimeout,
		RequiredAcks: kafka.RequireAll,
		BatchBytes:   int64(k.options.MaxMessageBytes),
		Async:        false,
	}
	compression := strings.ToLower(strings.TrimSpace(k.options.Compression))
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
			zap.String("compression", k.options.Compression))
		k.options.Compression = "none"
	}
	log.Info("Kafka producer uses " + k.options.Compression + " compression algorithm")
	return w
}

// SyncProducer creates a sync producer to writer message to kafka
func (k *Factory) SyncProducer() (pkafka.SyncProducer, error) {
	w := k.createWriter()
	return &syncWriter{w: w}, nil
}

// AsyncProducer creates an async producer to writer message to kafka
func (k *Factory) AsyncProducer(changefeedID model.ChangeFeedID,
	closedChan chan struct{},
	failpointCh chan error,
) (pkafka.AsyncProducer, error) {
	w := k.createWriter()
	aw := &asyncWriter{
		w:            w,
		closedChan:   closedChan,
		changefeedID: changefeedID,
		failpointCh:  failpointCh,
	}
	w.Completion = aw.callBackRun
	return aw, nil
}

// MetricRegistry returns the kafka client metric registry
func (k *Factory) MetricRegistry() metrics.Registry {
	return metrics.DefaultRegistry
}

// Close closes the client
func (k *Factory) Close() error {
	return nil
}

type syncWriter struct {
	w *kafka.Writer
}

func (s *syncWriter) SendMessage(
	ctx context.Context,
	topic string, partitionNum int32,
	key []byte, value []byte,
) error {
	return s.w.WriteMessages(ctx, kafka.Message{
		Topic:     topic,
		Partition: int(partitionNum),
		Key:       key,
		Value:     value,
	})
}

// SendMessages produces a given set of messages, and returns only when all
// messages in the set have either succeeded or failed. Note that messages
// can succeed and fail individually; if some succeed and some fail,
// SendMessages will return an error.
func (s *syncWriter) SendMessages(
	ctx context.Context,
	topic string, partitionNum int32,
	key []byte, value []byte,
) error {
	msgs := make([]kafka.Message, int(partitionNum))
	for i := 0; i < int(partitionNum); i++ {
		msgs[i] = kafka.Message{
			Topic:     topic,
			Key:       key,
			Value:     value,
			Partition: i,
		}
	}
	return s.w.WriteMessages(ctx, msgs...)
}

// Close shuts down the producer; you must call this function before a producer
// object passes out of scope, as it may otherwise leak memory.
// You must call this before calling Close on the underlying client.
func (s *syncWriter) Close() error {
	return s.w.Close()
}

type asyncWriter struct {
	w            *kafka.Writer
	changefeedID model.ChangeFeedID
	closedChan   chan struct{}
	failpointCh  chan error
	successes    chan []kafka.Message
	errorsChan   chan error
}

// Close shuts down the producer and waits for any buffered messages to be
// flushed. You must call this function before a producer object passes out of
// scope, as it may otherwise leak memory. You must call this before process
// shutting down, or you may lose messages. You must call this before calling
// Close on the underlying client.
func (a *asyncWriter) Close() error {
	return a.w.Close()
}

// AsyncSend is the input channel for the user to write messages to that they
// wish to send.
func (a *asyncWriter) AsyncSend(ctx context.Context, topic string,
	partition int32, key []byte, value []byte,
	callback func(),
) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case <-a.closedChan:
		log.Warn("Receive from closed chan in kafka producer",
			zap.String("namespace", a.changefeedID.Namespace),
			zap.String("changefeed", a.changefeedID.ID))
		return nil
	default:
	}
	return a.w.WriteMessages(context.Background(), kafka.Message{
		Topic:     topic,
		Partition: int(partition),
		Key:       key,
		Value:     value,
		Metadata:  callback,
	})
}

// AsyncRunCallback process the messages that has sent to kafka,
// and run tha attached callback. the caller should call this
// method in a background goroutine
func (a *asyncWriter) AsyncRunCallback(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-a.closedChan:
			log.Warn("Receive from closed chan in kafka producer",
				zap.String("namespace", a.changefeedID.Namespace),
				zap.String("changefeed", a.changefeedID.ID))
			return nil
		case err := <-a.failpointCh:
			log.Warn("Receive from failpoint chan in kafka producer",
				zap.String("namespace", a.changefeedID.Namespace),
				zap.String("changefeed", a.changefeedID.ID),
				zap.Error(err))
			return errors.Trace(err)
		case msgs := <-a.successes:
			for _, ack := range msgs {
				callback := ack.Metadata.(func())
				if callback != nil {
					callback()
				}
			}
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
}

func (a *asyncWriter) callBackRun(messages []kafka.Message, err error) {
	if err != nil {
		a.errorsChan <- err
	} else {
		a.successes <- messages
	}
}
