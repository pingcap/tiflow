// Copyright 2020 PingCAP, Inc.
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
	"github.com/pingcap/errors"
)

func GetPartitionCount(address []string, topic string, cfg *sarama.Config) (int32, error) {
	admin, err := sarama.NewClusterAdmin(address, cfg)
	if err != nil {
		return 0, errors.Trace(err)
	}
	topics, err := admin.ListTopics()
	if err != nil {
		return 0, errors.Trace(err)
	}
	err = admin.Close()
	if err != nil {
		return 0, errors.Trace(err)
	}
	topicDetail, exist := topics[topic]
	if !exist {
		return 0, errors.Errorf("can not find topic %s", topic)
	}
	return topicDetail.NumPartitions, nil
}
