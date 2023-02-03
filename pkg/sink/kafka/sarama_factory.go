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
	"github.com/Shopify/sarama"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/rcrowley/go-metrics"
)

type saramaFactory struct {
	client sarama.Client
}

func (f *saramaFactory) AdminClient() (ClusterAdminClient, error) {
	return nil, nil
}

func (f *saramaFactory) SyncProducer() (SyncProducer, error) {
	p, err := sarama.NewSyncProducerFromClient(f.client)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &saramaSyncProducer{producer: p}, nil
}

func (f *saramaFactory) AsyncProducer(
	changefeedID model.ChangeFeedID,
	closedChan chan struct{},
	failpointCh chan error,
) (AsyncProducer, error) {
	p, err := sarama.NewAsyncProducerFromClient(f.client)
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
	return f.client.Config().MetricRegistry
}

func (f *saramaFactory) Close() error {
	return f.client.Close()
}
