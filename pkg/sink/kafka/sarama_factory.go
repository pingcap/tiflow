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
	"github.com/labstack/gommon/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

type saramaFactory struct {
	changefeedID model.ChangeFeedID
	option       *Options
	config       *sarama.Config
}

// NewSaramaFactory constructs a Factory with sarama implementation.
func NewSaramaFactory(ctx context.Context,
	o *Options,
	changefeedID model.ChangeFeedID,
) (Factory, error) {
	saramaConfig, err := NewSaramaConfig(o)
	if err != nil {
		return nil, err
	}
	return &saramaFactory{
		changefeedID: changefeedID,
		option:       o,
		config:       saramaConfig,
	}, nil
}

func (f *saramaFactory) AdminClient() (ClusterAdminClient, error) {
	f.config.Producer.MaxMessageBytes = f.option.MaxMessageBytes
	return newAdminClient(f.option.BrokerEndpoints, f.config, f.changefeedID)
}

// SyncProducer returns a Sync Producer,
// it should be the caller's responsibility to close the producer
func (f *saramaFactory) SyncProducer() (SyncProducer, error) {
	f.config.Producer.MaxMessageBytes = f.option.MaxMessageBytes
	client, err := sarama.NewClient(f.option.BrokerEndpoints, f.config)
	if err != nil {
		return nil, errors.Trace(err)
	}

	p, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Info("kafka sync producer created",
		zap.String("namespace", f.changefeedID.Namespace),
		zap.String("changefeed", f.changefeedID.ID),
		zap.Any("brokers", f.option.BrokerEndpoints),
		zap.Any("config", f.config))

	return &saramaSyncProducer{
		id:       f.changefeedID,
		client:   client,
		producer: p,
	}, nil
}

// AsyncProducer return an Async Producer,
// it should be the caller's responsibility to close the producer
func (f *saramaFactory) AsyncProducer(
	closedChan chan struct{},
	failpointCh chan error,
) (AsyncProducer, error) {
	f.config.Producer.MaxMessageBytes = f.option.MaxMessageBytes
	client, err := sarama.NewClient(f.option.BrokerEndpoints, f.config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	p, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Info("kafka async producer created",
		zap.String("namespace", f.changefeedID.Namespace),
		zap.String("changefeed", f.changefeedID.ID),
		zap.Any("brokers", f.option.BrokerEndpoints),
		zap.Any("config", f.config))

	return &saramaAsyncProducer{
		client:       client,
		producer:     p,
		changefeedID: f.changefeedID,
		closedChan:   closedChan,
		failpointCh:  failpointCh,
	}, nil
}

func (f *saramaFactory) MetricsCollector(
	role util.Role,
	adminClient ClusterAdminClient,
) MetricsCollector {
	return NewSaramaMetricsCollector(
		f.changefeedID, role, adminClient, f.config.MetricRegistry)
}
