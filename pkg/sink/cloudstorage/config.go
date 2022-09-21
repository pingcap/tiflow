// Copyright 2022 PingCAP, Inc.
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

package cloudstorage

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	psink "github.com/pingcap/tiflow/pkg/sink"
	"go.uber.org/zap"
)

const (
	// defaultWorkerCount is the default number of workers.
	defaultWorkerCount = 16
	// the upper limit of max worker counts.
	maxWorkerCount = 512
	// defaultFlushInterval is the default flush interval for cloud storage sink.
	defaultFlushInterval = 5 * time.Second
	// the upper limit of flush interval.
	maxFlushInterval = 10 * time.Minute
)

// Config is the configuration for cloud storage sink.
type Config struct {
	WorkerCount   int
	FlushInterval time.Duration
}

// NewConfig returns the default cloud storage sink config.
func NewConfig() *Config {
	return &Config{
		WorkerCount:   defaultWorkerCount,
		FlushInterval: defaultFlushInterval,
	}
}

// Apply applies the sink URI parameters to the config.
func (c *Config) Apply(
	ctx context.Context,
	sinkURI *url.URL,
) (err error) {
	if sinkURI == nil {
		return cerror.ErrCloudStorageInvalidConfig.GenWithStack("failed to open cloud storage sink, empty SinkURI")
	}

	scheme := strings.ToLower(sinkURI.Scheme)
	if !psink.IsStorageScheme(scheme) {
		return cerror.ErrCloudStorageInvalidConfig.GenWithStack("can't create cloud storage sink with unsupported scheme: %s", scheme)
	}
	query := sinkURI.Query()
	if err = getWorkerCount(query, &c.WorkerCount); err != nil {
		return err
	}
	err = getFlushInterval(query, &c.FlushInterval)

	return err
}

func getWorkerCount(values url.Values, workerCount *int) error {
	s := values.Get("worker-count")
	if len(s) == 0 {
		return nil
	}

	c, err := strconv.Atoi(s)
	if err != nil {
		return cerror.WrapError(cerror.ErrCloudStorageInvalidConfig, err)
	}
	if c <= 0 {
		return cerror.WrapError(cerror.ErrCloudStorageInvalidConfig,
			fmt.Errorf("invalid worker-count %d, it must be greater than 0", c))
	}
	if c > maxWorkerCount {
		log.Warn("worker-count is too large",
			zap.Int("original", c), zap.Int("override", maxWorkerCount))
		c = maxWorkerCount
	}

	*workerCount = c
	return nil
}

func getFlushInterval(values url.Values, flushInterval *time.Duration) error {
	s := values.Get("flush-interval")
	if len(s) == 0 {
		return nil
	}

	d, err := time.ParseDuration(s)
	if err != nil {
		return cerror.WrapError(cerror.ErrCloudStorageInvalidConfig, err)
	}
	if d <= 0 {
		return cerror.WrapError(cerror.ErrCloudStorageInvalidConfig,
			fmt.Errorf("invalid flush-interval %s, it must be greater than 0", d))
	}
	if d > maxFlushInterval {
		log.Warn("flush-interval is too large", zap.Duration("original", d),
			zap.Duration("override", maxFlushInterval))
		d = maxFlushInterval
	}

	*flushInterval = d
	return nil
}
