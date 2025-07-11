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
	"time"

	"github.com/IBM/sarama"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/rcrowley/go-metrics"
	"go.uber.org/zap"
)

type saramaFactory struct {
	changefeedID model.ChangeFeedID
	option       *Options

	registry metrics.Registry
}

// NewSaramaFactory constructs a Factory with sarama implementation.
func NewSaramaFactory(
	o *Options,
	changefeedID model.ChangeFeedID,
) (Factory, error) {
	return &saramaFactory{
		changefeedID: changefeedID,
		option:       o,
		registry:     metrics.NewRegistry(),
	}, nil
}

func (f *saramaFactory) AdminClient(ctx context.Context) (ClusterAdminClient, error) {
	start := time.Now()
	config, err := NewSaramaConfig(ctx, f.option)
	duration := time.Since(start).Seconds()
	if duration > 2 {
		log.Warn("new sarama config cost too much time", zap.Any("duration", duration), zap.Stringer("changefeedID", f.changefeedID))
	}
	if err != nil {
		return nil, err
	}

	start = time.Now()
	client, err := sarama.NewClient(f.option.BrokerEndpoints, config)
	duration = time.Since(start).Seconds()
	if duration > 2 {
		log.Warn("new sarama client cost too much time", zap.Any("duration", duration), zap.Stringer("changefeedID", f.changefeedID))
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	start = time.Now()
	admin, err := sarama.NewClusterAdminFromClient(client)
	duration = time.Since(start).Seconds()
	if duration > 2 {
		log.Warn("new sarama cluster admin cost too much time", zap.Any("duration", duration), zap.Stringer("changefeedID", f.changefeedID))
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &saramaAdminClient{
		client:     client,
		admin:      admin,
		changefeed: f.changefeedID,
	}, nil
}

// SyncProducer returns a Sync Producer,
// it should be the caller's responsibility to close the producer
func (f *saramaFactory) SyncProducer(ctx context.Context) (SyncProducer, error) {
	config, err := NewSaramaConfig(ctx, f.option)
	if err != nil {
		return nil, err
	}
	config.MetricRegistry = f.registry

	client, err := sarama.NewClient(f.option.BrokerEndpoints, config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	p, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &saramaSyncProducer{
		id:                    f.changefeedID,
		producer:              p,
		client:                client,
		keepConnAliveInterval: f.option.KeepConnAliveInterval,
		lastHeartbeatTime:     time.Now().Add(-f.option.KeepConnAliveInterval),
	}, nil
}

// AsyncProducer return an Async Producer,
// it should be the caller's responsibility to close the producer
func (f *saramaFactory) AsyncProducer(
	ctx context.Context,
	failpointCh chan error,
) (AsyncProducer, error) {
	config, err := NewSaramaConfig(ctx, f.option)
	if err != nil {
		return nil, err
	}
	config.MetricRegistry = f.registry

	client, err := sarama.NewClient(f.option.BrokerEndpoints, config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	p, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &saramaAsyncProducer{
		client:                client,
		producer:              p,
		changefeedID:          f.changefeedID,
		keepConnAliveInterval: f.option.KeepConnAliveInterval,
		failpointCh:           failpointCh,
	}, nil
}

func (f *saramaFactory) MetricsCollector(
	role util.Role,
	adminClient ClusterAdminClient,
) MetricsCollector {
	return NewSaramaMetricsCollector(
		f.changefeedID, role, adminClient, f.registry)
}
