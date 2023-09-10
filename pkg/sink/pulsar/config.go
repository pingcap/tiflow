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

package pulsar

import (
	"fmt"
	"net/url"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/config"
	"go.uber.org/zap"
)

// sink config default Value
const (
	defaultConnectionTimeout = 5 // 5s

	defaultOperationTimeout = 30 // 30s

	defaultBatchingMaxSize = uint(1000)

	defaultBatchingMaxPublishDelay = 10 // 10ms

	// defaultSendTimeout 30s
	defaultSendTimeout = 30 // 30s

)

func checkSinkURI(sinkURI *url.URL) error {
	if sinkURI.Scheme == "" {
		return fmt.Errorf("scheme is empty")
	}
	if sinkURI.Host == "" {
		return fmt.Errorf("host is empty")
	}
	if sinkURI.Path == "" {
		return fmt.Errorf("path is empty")
	}
	return nil
}

// NewPulsarConfig new pulsar config
// TODO(dongmen): make this method more concise.
func NewPulsarConfig(sinkURI *url.URL, pulsarConfig *config.PulsarConfig) (*config.PulsarConfig, error) {
	c := &config.PulsarConfig{
		ConnectionTimeout:       toSec(defaultConnectionTimeout),
		OperationTimeout:        toSec(defaultOperationTimeout),
		BatchingMaxMessages:     toUint(defaultBatchingMaxSize),
		BatchingMaxPublishDelay: toMill(defaultBatchingMaxPublishDelay),
		SendTimeout:             toSec(defaultSendTimeout),
	}
	err := checkSinkURI(sinkURI)
	if err != nil {
		return nil, err
	}

	c.SinkURI = sinkURI
	c.BrokerURL = sinkURI.Scheme + "://" + sinkURI.Host

	if pulsarConfig == nil {
		log.L().Debug("new pulsar config", zap.Any("config", c))
		return c, nil
	}

	pulsarConfig.SinkURI = c.SinkURI

	if len(sinkURI.Scheme) == 0 || len(sinkURI.Host) == 0 {
		return nil, fmt.Errorf("BrokerURL is empty")
	}
	pulsarConfig.BrokerURL = c.BrokerURL

	// merge default config
	if pulsarConfig.ConnectionTimeout == nil {
		pulsarConfig.ConnectionTimeout = c.ConnectionTimeout
	}
	if pulsarConfig.OperationTimeout == nil {
		pulsarConfig.OperationTimeout = c.OperationTimeout
	}
	if pulsarConfig.BatchingMaxMessages == nil {
		pulsarConfig.BatchingMaxMessages = c.BatchingMaxMessages
	}
	if pulsarConfig.BatchingMaxPublishDelay == nil {
		pulsarConfig.BatchingMaxPublishDelay = c.BatchingMaxPublishDelay
	}
	if pulsarConfig.SendTimeout == nil {
		pulsarConfig.SendTimeout = c.SendTimeout
	}

	log.L().Debug("new pulsar config success", zap.Any("config", pulsarConfig))

	return pulsarConfig, nil
}

func toSec(x int) *config.TimeSec {
	t := config.TimeSec(x)
	return &t
}

func toMill(x int) *config.TimeMill {
	t := config.TimeMill(x)
	return &t
}

func toUint(x uint) *uint {
	return &x
}
