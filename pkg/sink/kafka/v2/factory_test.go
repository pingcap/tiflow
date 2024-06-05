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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	pkafka "github.com/pingcap/tiflow/pkg/sink/kafka"
	v2mock "github.com/pingcap/tiflow/pkg/sink/kafka/v2/mock"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/stretchr/testify/require"
)

func newOptions4Test() *pkafka.Options {
	o := pkafka.NewOptions()
	o.BrokerEndpoints = []string{"127.0.0.1:9092"}
	o.ClientID = "kafka-go-test"
	o.EnableTLS = true
	o.Credential = &security.Credential{
		CAPath:        "",
		CertPath:      "",
		KeyPath:       "",
		CertAllowedCN: []string{""},
	}
	return o
}

func newFactory4Test(o *pkafka.Options, t *testing.T) *factory {
	f, err := NewFactory(o, model.DefaultChangeFeedID("kafka-go-sink"))
	require.NoError(t, err)

	return f.(*factory)
}

func TestSyncProducer(t *testing.T) {
	t.Parallel()

	o := newOptions4Test()
	factory := newFactory4Test(o, t)

	sync, err := factory.SyncProducer(context.Background())
	require.NoError(t, err)

	p, ok := sync.(*syncWriter)
	require.True(t, ok)
	require.False(t, p.w.(*kafka.Writer).Async)
}

func TestCompression(t *testing.T) {
	t.Parallel()

	o := newOptions4Test()
	factory := newFactory4Test(o, t)
	factory.newWriter(false)
	cases := []struct {
		compression string
		expected    kafka.Compression
	}{
		{"none", 0},
		{"gzip", kafka.Gzip},
		{"snappy", kafka.Snappy},
		{"lz4", kafka.Lz4},
		{"zstd", kafka.Zstd},
		{"xxxx", 0},
	}
	for _, cs := range cases {
		o.Compression = cs.compression
		w := factory.newWriter(false)
		require.Equal(t, cs.expected, w.Compression)
	}
}

func TestAsyncProducer(t *testing.T) {
	t.Parallel()

	o := newOptions4Test()
	factory := newFactory4Test(o, t)
	require.Equal(
		t, factory.transport.TLS,
		&tls.Config{
			MinVersion: tls.VersionTLS12,
			NextProtos: []string{"h2", "http/1.1"},
		},
	)

	ctx := context.Background()
	async, err := factory.AsyncProducer(ctx, make(chan error, 1))
	require.NoError(t, err)

	asyncP, ok := async.(*asyncWriter)
	w := asyncP.w.(*kafka.Writer)
	require.True(t, ok)
	require.True(t, w.Async)

	require.Equal(t, w.ReadTimeout, o.ReadTimeout)
	require.Equal(t, w.WriteTimeout, o.WriteTimeout)
	require.Equal(t, w.RequiredAcks, kafka.RequiredAcks(o.RequiredAcks))
	require.Equal(t, w.BatchBytes, int64(o.MaxMessageBytes))
}

func TestAsyncCompletion(t *testing.T) {
	o := newOptions4Test()
	factory := newFactory4Test(o, t)
	ctx := context.Background()
	async, err := factory.AsyncProducer(ctx, make(chan error, 1))
	require.NoError(t, err)
	asyncP, ok := async.(*asyncWriter)
	require.True(t, ok)
	w := asyncP.w.(*kafka.Writer)
	acked := 0
	callback := func() {
		acked++
	}
	msgs := []kafka.Message{
		{
			WriterData: callback,
		},
		{
			WriterData: callback,
		},
	}
	w.Completion(msgs, nil)
	require.Equal(t, 2, acked)
	asyncP.errorsChan = make(chan error, 2)
	w.Completion(msgs, errors.New("fake"))
	require.Equal(t, 1, len(asyncP.errorsChan))
	asyncP.errorsChan <- errors.New("fake 2")
	w.Completion(msgs, errors.New("fake"))
	require.Equal(t, 2, len(asyncP.errorsChan))
	require.Equal(t, 2, acked)
}

func TestNewMetricsCollector(t *testing.T) {
	require.NotNil(t, NewMetricsCollector(model.DefaultChangeFeedID("1"), util.RoleOwner, nil))
}

func TestCompleteSASLConfig(t *testing.T) {
	m, err := completeSASLConfig(&pkafka.Options{
		SASL: nil,
	})
	require.Nil(t, m)
	require.Nil(t, err)
	m, err = completeSASLConfig(&pkafka.Options{
		SASL: &security.SASL{
			SASLUser:      "user",
			SASLPassword:  "pass",
			SASLMechanism: pkafka.SASLTypeSCRAMSHA256,
		},
	})
	require.Nil(t, err)
	require.Equal(t, pkafka.SASLTypeSCRAMSHA256, m.Name())
	m, err = completeSASLConfig(&pkafka.Options{
		SASL: &security.SASL{
			SASLUser:      "user",
			SASLPassword:  "pass",
			SASLMechanism: pkafka.SASLTypeSCRAMSHA512,
		},
	})
	require.NotNil(t, m)
	require.Equal(t, pkafka.SASLTypeSCRAMSHA512, m.Name())
	require.Nil(t, err)
	require.Equal(t, pkafka.SASLTypeSCRAMSHA512, m.Name())
	m, err = completeSASLConfig(&pkafka.Options{
		SASL: &security.SASL{
			SASLUser:      "user",
			SASLPassword:  "pass",
			SASLMechanism: pkafka.SASLTypePlaintext,
		},
	})
	pm, ok := m.(plain.Mechanism)
	require.True(t, ok)
	require.Nil(t, err)
	require.Equal(t, pkafka.SASLTypePlaintext, m.Name())
	require.Equal(t, "user", pm.Username)
	require.Equal(t, "pass", pm.Password)

	// Unsupported OAUTHBEARER mechanism
	m, err = completeSASLConfig(&pkafka.Options{
		SASL: &security.SASL{
			SASLMechanism: security.OAuthMechanism,
		},
	})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "OAuth is not yet supported in Kafka sink v2")
}

func TestSyncWriterSendMessage(t *testing.T) {
	mw := v2mock.NewMockWriter(gomock.NewController(t))
	w := syncWriter{w: mw}
	mw.EXPECT().WriteMessages(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, msgs ...kafka.Message) error {
			require.Equal(t, 1, len(msgs))
			require.Equal(t, 3, msgs[0].Partition)
			return errors.New("fake")
		})

	message := &common.Message{Key: []byte{'1'}, Value: []byte{}}
	require.NotNil(t, w.SendMessage(context.Background(), "topic", 3, message))
}

func TestSyncWriterSendMessages(t *testing.T) {
	mw := v2mock.NewMockWriter(gomock.NewController(t))
	w := syncWriter{w: mw}
	mw.EXPECT().WriteMessages(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, msgs ...kafka.Message) error {
			require.Equal(t, 3, len(msgs))
			return errors.New("fake")
		})
	message := &common.Message{Key: []byte{'1'}, Value: []byte{}}
	require.NotNil(t, w.SendMessages(context.Background(), "topic", 3, message))
}

func TestSyncWriterClose(t *testing.T) {
	mw := v2mock.NewMockWriter(gomock.NewController(t))
	w := syncWriter{w: mw}
	// close failed,no panic
	mw.EXPECT().Close().Return(errors.New("fake"))
	w.Close()
	// close success
	mw.EXPECT().Close().Return(nil)
	w.Close()
}

func TestAsyncWriterAsyncSend(t *testing.T) {
	mw := v2mock.NewMockWriter(gomock.NewController(t))
	w := asyncWriter{w: mw}

	ctx, cancel := context.WithCancel(context.Background())

	message := &common.Message{Key: []byte{'1'}, Value: []byte{}, Callback: func() {}}
	mw.EXPECT().WriteMessages(gomock.Any(), gomock.Any()).Return(nil)
	err := w.AsyncSend(ctx, "topic", 1, message)
	require.NoError(t, err)

	cancel()

	err = w.AsyncSend(ctx, "topic", 1, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestAsyncProducerErrorChan(t *testing.T) {
	t.Parallel()

	o := newOptions4Test()
	factory := newFactory4Test(o, t)

	ctx := context.Background()
	asyncProducer, err := factory.AsyncProducer(ctx, make(chan error, 1))
	require.NoError(t, err)

	mockErr := cerror.New("errors chan error")
	go func() {
		err = asyncProducer.AsyncRunCallback(ctx)
		require.Equal(t, err.Error(), cerror.WrapError(cerror.ErrKafkaAsyncSendMessage, mockErr).Error())
	}()

	asyncProducer.(*asyncWriter).errorsChan <- mockErr
}
