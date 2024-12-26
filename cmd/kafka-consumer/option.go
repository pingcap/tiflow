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

package main

import (
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	cerror "github.com/pingcap/errors"
	"github.com/pingcap/log"
	cmdUtil "github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/pingcap/tiflow/pkg/config"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

var (
	defaultVersion   = "2.4.0"
	defaultRetryTime = 30
	defaultTimeout   = time.Second * 10
)

type option struct {
	address      []string
	version      string
	topic        string
	partitionNum int32
	groupID      string

	maxMessageBytes int
	maxBatchSize    int

	protocol config.Protocol

	codecConfig *common.Config
	// the replicaConfig of the changefeed which produce data to the kafka topic
	replicaConfig *config.ReplicaConfig

	logPath       string
	logLevel      string
	timezone      string
	ca, cert, key string

	downstreamURI string

	// avro schema registry uri should be set if the encoding protocol is avro
	schemaRegistryURI string

	// upstreamTiDBDSN is the dsn of the upstream TiDB cluster
	upstreamTiDBDSN string

	enableProfiling bool

	// connect kafka retry times, default 30
	retryTime int
	// connect kafka  timeout, default 10s
	timeout time.Duration
}

func newOption() *option {
	return &option{
		version:         defaultVersion,
		maxMessageBytes: math.MaxInt64,
		maxBatchSize:    math.MaxInt64,
		retryTime:       defaultRetryTime,
		timeout:         defaultTimeout,
	}
}

// Adjust the consumer option by the upstream uri passed in parameters.
func (o *option) Adjust(upstreamURI *url.URL, configFile string) error {
	s := upstreamURI.Query().Get("version")
	if s != "" {
		o.version = s
	}
	o.topic = strings.TrimFunc(upstreamURI.Path, func(r rune) bool {
		return r == '/'
	})
	o.address = strings.Split(upstreamURI.Host, ",")

	s = upstreamURI.Query().Get("partition-num")
	if s != "" {
		c, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			log.Panic("invalid partition-num of upstream-uri")
		}
		o.partitionNum = int32(c)
	}

	s = upstreamURI.Query().Get("max-message-bytes")
	if s != "" {
		c, err := strconv.Atoi(s)
		if err != nil {
			log.Panic("invalid max-message-bytes of upstream-uri")
		}
		o.maxMessageBytes = c
	}

	s = upstreamURI.Query().Get("max-batch-size")
	if s != "" {
		c, err := strconv.Atoi(s)
		if err != nil {
			log.Panic("invalid max-batch-size of upstream-uri")
		}
		o.maxBatchSize = c
	}

	s = upstreamURI.Query().Get("protocol")
	if s == "" {
		log.Panic("cannot found the protocol from the sink url")
	}
	protocol, err := config.ParseSinkProtocolFromString(s)
	if err != nil {
		log.Panic("invalid protocol", zap.Error(err), zap.String("protocol", s))
	}
	o.protocol = protocol

	replicaConfig := config.GetDefaultReplicaConfig()
	// the TiDB source ID should never be set to 0
	replicaConfig.Sink.TiDBSourceID = 1
	replicaConfig.Sink.Protocol = util.AddressOf(protocol.String())
	if configFile != "" {
		err = cmdUtil.StrictDecodeFile(configFile, "kafka consumer", replicaConfig)
		if err != nil {
			return cerror.Trace(err)
		}
		if _, err = filter.VerifyTableRules(replicaConfig.Filter); err != nil {
			return cerror.Trace(err)
		}
	}
	o.replicaConfig = replicaConfig

	o.codecConfig = common.NewConfig(protocol)
	if err = o.codecConfig.Apply(upstreamURI, o.replicaConfig); err != nil {
		return cerror.Trace(err)
	}
	tz, err := util.GetTimezone(o.timezone)
	if err != nil {
		return cerrors.Trace(err)
	}
	o.codecConfig.TimeZone = tz

	if protocol == config.ProtocolAvro {
		o.codecConfig.AvroEnableWatermark = true
	}

	log.Info("consumer option adjusted",
		zap.String("configFile", configFile),
		zap.String("address", strings.Join(o.address, ",")),
		zap.String("version", o.version),
		zap.String("topic", o.topic),
		zap.Int32("partitionNum", o.partitionNum),
		zap.String("groupID", o.groupID),
		zap.Int("maxMessageBytes", o.maxMessageBytes),
		zap.Int("maxBatchSize", o.maxBatchSize),
		zap.String("upstreamURI", upstreamURI.String()),
		zap.String("downstreamURI", o.downstreamURI))
	return nil
}
