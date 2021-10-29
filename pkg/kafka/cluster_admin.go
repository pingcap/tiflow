// Copyright 2021 PingCAP, Inc.
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
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type ClusterAdmin struct {
	sarama.ClusterAdmin
	topics map[string]sarama.TopicDetail
}

func NewClusterAdmin(address []string, cfg *sarama.Config) (*ClusterAdmin, error) {
	admin, err := sarama.NewClusterAdmin(address, cfg)
	if err != nil {
		return nil, err
	}

	return &ClusterAdmin{
		ClusterAdmin: admin,
		topics:       make(map[string]sarama.TopicDetail),
	}, nil
}

func (admin *ClusterAdmin) Close() error {
	return admin.ClusterAdmin.Close()
}

func (admin *ClusterAdmin) SyncTopics() error {
	topics, err := admin.ListTopics()
	if err != nil {
		return errors.Trace(err)
	}

	admin.topics = topics
	return nil
}

func (admin *ClusterAdmin) WaitTopicCreated(topic string) error {
	for i := 0; i <= 30; i++ {
		if err := admin.SyncTopics(); err != nil {
			return errors.Trace(err)
		}
		if _, ok := admin.topics[topic]; ok {
			return nil
		}
		log.Info("wait the topic created", zap.String("topic", topic))
		time.Sleep(1 * time.Second)
	}
	return errors.Errorf("wait the topic(%s) created timeout", topic)
}

func (admin *ClusterAdmin) GetPartitionCount(topic string) (int32, error) {
	if err := admin.SyncTopics(); err != nil {
		return 0, errors.Trace(err)
	}

	detail, ok := admin.topics[topic]
	if ok {
		return detail.NumPartitions, nil
	}
	return 0, errors.Errorf("topic %s not found", topic)
}
