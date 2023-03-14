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

	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	pkafka "github.com/pingcap/tiflow/pkg/sink/kafka"
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

	sync, err := factory.SyncProducer()
	require.NoError(t, err)

	p, ok := sync.(*syncWriter)
	require.True(t, ok)
	require.False(t, p.w.(*kafka.Writer).Async)
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
	async, err := factory.AsyncProducer(ctx, make(chan struct{}, 1), make(chan error, 1))
	require.NoError(t, err)

	asyncP, ok := async.(*asyncWriter)
	w := asyncP.w.(*kafka.Writer)
	require.True(t, ok)
	require.True(t, w.Async)
	require.NotNil(t, asyncP.closedChan)

	require.Equal(t, w.ReadTimeout, o.ReadTimeout)
	require.Equal(t, w.WriteTimeout, o.WriteTimeout)
	require.Equal(t, w.RequiredAcks, kafka.RequiredAcks(o.RequiredAcks))
	require.Equal(t, w.BatchBytes, int64(o.MaxMessageBytes))

	var (
		async0, _ = factory.AsyncProducer(
			ctx,
			make(chan struct{}, 1),
			make(chan error, 1),
		)
		asyncP0, _ = async0.(*asyncWriter)
		retErr0    error
		retChan0   = make(chan struct{})
	)
	go func() {
		retErr0 = asyncP0.AsyncRunCallback(ctx)
		close(retChan0)
	}()
	close(asyncP0.closedChan)
	<-retChan0
	require.NoError(t, retErr0)

	var (
		async1, _ = factory.AsyncProducer(
			ctx,
			make(chan struct{}, 1),
			make(chan error, 1),
		)
		asyncP1, _ = async1.(*asyncWriter)
		retErr1    error
		retChan1   = make(chan struct{})
	)
	go func() {
		retErr1 = asyncP1.AsyncRunCallback(ctx)
		close(retChan1)
	}()
	sendErr := cerror.New("failed point error")
	asyncP1.failpointCh <- sendErr
	<-retChan1
	require.Equal(t, retErr1.Error(), cerror.Trace(sendErr).Error())

	var (
		async2, _ = factory.AsyncProducer(
			ctx,
			make(chan struct{}, 1),
			make(chan error, 1),
		)
		asyncP2, _ = async2.(*asyncWriter)
		retErr2    error
		retChan2   = make(chan struct{})
	)
	go func() {
		retErr2 = asyncP2.AsyncRunCallback(ctx)
		close(retChan2)
	}()
	sendErr2 := cerror.New("errors chan error")
	asyncP2.errorsChan <- sendErr2
	<-retChan2
	require.Equal(
		t, retErr2.Error(),
		cerror.WrapError(
			cerror.ErrKafkaAsyncSendMessage, sendErr2,
		).Error(),
	)

	var (
		async3, _ = factory.AsyncProducer(
			ctx,
			make(chan struct{}, 1),
			make(chan error, 1),
		)
		asyncP3, _ = async3.(*asyncWriter)
		retErr3    error
		retChan3   = make(chan struct{})
	)
	go func() {
		retErr3 = asyncP3.AsyncRunCallback(ctx)
		close(retChan3)
	}()
	close(asyncP3.errorsChan)
	<-retChan3
	require.NoError(t, retErr3)

	var (
		async4, _ = factory.AsyncProducer(
			ctx,
			make(chan struct{}, 1),
			make(chan error, 1),
		)
		asyncP4, _    = async4.(*asyncWriter)
		retErr4       error
		retChan4      = make(chan struct{})
		ctx4, cancel4 = context.WithCancel(context.Background())
	)
	go func() {
		retErr4 = asyncP4.AsyncRunCallback(ctx4)
		close(retChan4)
	}()
	cancel4()
	<-retChan4
	require.Equal(
		t, retErr4.Error(),
		cerror.Trace(ctx4.Err()).Error(),
	)
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
	require.Nil(t, m)
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
}
