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

	"github.com/Shopify/sarama"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/rcrowley/go-metrics"
)

type saramaFactory struct {
	brokerEndpoints []string
	config          *sarama.Config
}

// NewSaramaFactory constructs a Factory with sarama implementation.
func NewSaramaFactory(ctx context.Context, o *Options) (Factory, error) {
	saramaConfig, err := NewSaramaConfig(ctx, o)
	if err != nil {
		return nil, err
	}
	return &saramaFactory{
		brokerEndpoints: o.BrokerEndpoints,
		config:          saramaConfig,
	}, nil
}

func (f *saramaFactory) AdminClient(ctx context.Context) (ClusterAdminClient, error) {
	return NewSaramaAdminClient(ctx, f.brokerEndpoints, f.config)
}

// SyncProducer return a Sync Producer,
// it should be the caller's responsibility to close the producer
func (f *saramaFactory) SyncProducer() (SyncProducer, error) {
	p, err := sarama.NewSyncProducer(f.brokerEndpoints, f.config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &saramaSyncProducer{producer: p}, nil
}

// AsyncProducer return an Async Producer,
// it should be the caller's responsibility to close the producer
func (f *saramaFactory) AsyncProducer(
	changefeedID model.ChangeFeedID,
	closedChan chan struct{},
	failpointCh chan error,
) (AsyncProducer, error) {
	p, err := sarama.NewAsyncProducer(f.brokerEndpoints, f.config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &saramaAsyncProducer{
		producer:     p,
		changefeedID: changefeedID,
		closedChan:   closedChan,
		failpointCh:  failpointCh,
	}, nil
}

func (f *saramaFactory) MetricRegistry() metrics.Registry {
	return f.config.MetricRegistry
}

func (f *saramaFactory) MetricsCollector(
	changefeedID model.ChangeFeedID,
	role util.Role,
	adminClient ClusterAdminClient,
) MetricsCollector {
	return NewSaramaMetricsCollector(
		changefeedID, role, adminClient, f.config.MetricRegistry)
}
