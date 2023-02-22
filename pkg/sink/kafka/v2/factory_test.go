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
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	pkafka "github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

func newOptions4Test() *pkafka.Options {
	o := pkafka.NewOptions()
	o.BrokerEndpoints = []string{"127.0.0.1:9092"}
	o.ClientID = "kafka-go-test"
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
	require.False(t, p.w.Async)
}

func TestAsyncProducer(t *testing.T) {
	t.Parallel()

	o := newOptions4Test()
	factory := newFactory4Test(o, t)

	async, err := factory.AsyncProducer(make(chan struct{}, 1), make(chan error, 1))
	require.NoError(t, err)

	asyncP, ok := async.(*asyncWriter)
	require.True(t, ok)
	require.True(t, asyncP.w.Async)
	require.NotNil(t, asyncP.successes)
	require.NotNil(t, asyncP.closedChan)

	require.Equal(t, asyncP.w.ReadTimeout, o.ReadTimeout)
	require.Equal(t, asyncP.w.WriteTimeout, o.WriteTimeout)
	require.Equal(t, asyncP.w.RequiredAcks, kafka.RequiredAcks(o.RequiredAcks))
	require.Equal(t, asyncP.w.BatchBytes, int64(o.MaxMessageBytes))
}
