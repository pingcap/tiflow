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
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	pkafka "github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/stretchr/testify/require"
)

func newOptions4Test() *pkafka.Options {
	o := pkafka.NewOptions()
	o.BrokerEndpoints = []string{"127.0.0.1:9092"}
	o.ClientID = "kafka-go-test"
	return o
}

func newFactory4Test(o *pkafka.Options, t *testing.T) *factory {
	f, err := NewFactory(context.Background(),
		o, model.DefaultChangeFeedID("kafka-go-sink"))
	require.NoError(t, err)

	return f.(*factory)
}

func TestNewWriter(t *testing.T) {
	t.Parallel()

	o := newOptions4Test()
	factory := newFactory4Test(o, t)

	sync := factory.newWriter(false)
	require.False(t, sync.Async)

	async := factory.newWriter(true)
	require.True(t, async.Async)

	require.Equal(t, async.ReadTimeout, o.ReadTimeout)
	require.Equal(t, async.WriteTimeout, o.WriteTimeout)
	require.Equal(t, async.RequiredAcks, o.RequiredAcks)
	require.Equal(t, async.BatchBytes, o.MaxMessageBytes)
}
