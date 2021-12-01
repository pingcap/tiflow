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
	"net/url"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
	"go.uber.org/zap"
)

// Config stores user specified Kafka producer configuration
type Config struct {
	BrokerEndpoints []string
	PartitionNum    int32

	// User should make sure that `replication-factor` not greater than the number of kafka brokers.
	ReplicationFactor int16

	Version         string
	MaxMessageBytes int
	Compression     string
	ClientID        string
	Credential      *security.Credential
	SaslScram       *security.SaslScram
	// control whether to create topic
	AutoCreate bool
}

// NewConfig returns a default Kafka configuration
func NewConfig() *Config {
	return &Config{
		Version: "2.4.0",
		// MaxMessageBytes will be used to initialize producer, we set the default value (1M) identical to kafka broker.
		MaxMessageBytes:   1 * 1024 * 1024,
		ReplicationFactor: 1,
		Compression:       "none",
		Credential:        &security.Credential{},
		SaslScram:         &security.SaslScram{},
		AutoCreate:        true,
	}
}

// CompleteByOpts the kafka configuration.
func (c *Config) CompleteByOpts(sinkURI *url.URL, replicaConfig *config.ReplicaConfig, opts map[string]string) error {
	c.BrokerEndpoints = strings.Split(sinkURI.Host, ",")
	params := sinkURI.Query()
	s := params.Get("partition-num")
	if s != "" {
		a, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		c.PartitionNum = int32(a)
		if c.PartitionNum <= 0 {
			return cerror.ErrKafkaInvalidPartitionNum.GenWithStackByArgs(c.PartitionNum)
		}
	}

	s = params.Get("replication-factor")
	if s != "" {
		a, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		c.ReplicationFactor = int16(a)
	}

	s = params.Get("kafka-version")
	if s != "" {
		c.Version = s
	}

	s = params.Get("max-message-bytes")
	if s != "" {
		a, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		c.MaxMessageBytes = a
		opts["max-message-bytes"] = s
	}

	s = params.Get("max-batch-size")
	if s != "" {
		opts["max-batch-size"] = s
	}

	s = params.Get("compression")
	if s != "" {
		c.Compression = s
	}

	c.ClientID = params.Get("kafka-client-id")

	s = params.Get("ca")
	if s != "" {
		c.Credential.CAPath = s
	}

	s = params.Get("cert")
	if s != "" {
		c.Credential.CertPath = s
	}

	s = params.Get("key")
	if s != "" {
		c.Credential.KeyPath = s
	}

	s = params.Get("sasl-user")
	if s != "" {
		c.SaslScram.SaslUser = s
	}

	s = params.Get("sasl-password")
	if s != "" {
		c.SaslScram.SaslPassword = s
	}

	s = params.Get("sasl-mechanism")
	if s != "" {
		c.SaslScram.SaslMechanism = s
	}

	s = params.Get("auto-create-topic")
	if s != "" {
		autoCreate, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		c.AutoCreate = autoCreate
	}

	s = params.Get("protocol")
	if s != "" {
		replicaConfig.Sink.Protocol = s
	}

	s = params.Get("enable-tidb-extension")
	if s != "" {
		_, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		if replicaConfig.Sink.Protocol != "canal-json" {
			return cerror.WrapError(cerror.ErrKafkaInvalidConfig, errors.New("enable-tidb-extension only support canal-json protocol"))
		}
		opts["enable-tidb-extension"] = s
	}

	return nil
}

// set the partition-num by the topic's partition count.
func (c *Config) setPartitionNum(realPartitionCount int32) error {
	// user does not specify the `partition-num` in the sink-uri
	if c.PartitionNum == 0 {
		c.PartitionNum = realPartitionCount
		return nil
	}

	if c.PartitionNum < realPartitionCount {
		log.Warn("number of partition specified in sink-uri is less than that of the actual topic. "+
			"Some partitions will not have messages dispatched to",
			zap.Int32("sink-uri partitions", c.PartitionNum),
			zap.Int32("topic partitions", realPartitionCount))
		return nil
	}

	// Make sure that the user-specified `partition-num` is not greater than
	// the real partition count, since messages would be dispatched to different
	// partitions, this could prevent potential correctness problems.
	if c.PartitionNum > realPartitionCount {
		return cerror.ErrKafkaInvalidPartitionNum.GenWithStack(
			"the number of partition (%d) specified in sink-uri is more than that of actual topic (%d)",
			c.PartitionNum, realPartitionCount)
	}
	return nil
}
