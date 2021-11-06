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
	"strings"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
)

type Admin struct {
	sarama.ClusterAdmin
}

func NewAdmin(address []string, cfg *sarama.Config) (*Admin, error) {
	admin, err := sarama.NewClusterAdmin(address, cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Admin{
		ClusterAdmin: admin,
	}, nil
}

func (admin *Admin) Close() error {
	return admin.ClusterAdmin.Close()
}

// GetPartitionCount return the partition number of the topic, return 0 if not found the topic.
func (admin *Admin) GetPartitionCount(topic string) (int32, error) {
	topics, err := admin.ListTopics()
	if err != nil {
		return 0, errors.Trace(err)
	}

	detail, ok := topics[topic]
	if !ok {
		return 0, nil
	}

	return detail.NumPartitions, nil
}

func (admin *Admin) CreateTopic(topic string, detail *sarama.TopicDetail) error {
	if err := admin.ClusterAdmin.CreateTopic(topic, detail, false); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return errors.Trace(err)
		}
	}
	return nil
}
