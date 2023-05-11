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
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	psink "github.com/pingcap/tiflow/pkg/sink"
	"go.uber.org/zap"
)

const (
	// defaultWorkerCount is the default value of worker-count.
	defaultWorkerCount = 16
	// the upper limit of worker-count.
	maxWorkerCount = 512
	// defaultFlushInterval is the default value of flush-interval.
	defaultFlushInterval = 5 * time.Second
	// the lower limit of flush-interval.
	minFlushInterval = 2 * time.Second
	// the upper limit of flush-interval.
	maxFlushInterval = 10 * time.Minute
	// defaultFileSize is the default value of file-size.
	defaultFileSize = 64 * 1024 * 1024
	// the lower limit of file size
	minFileSize = 1024 * 1024
	// the upper limit of file size
	maxFileSize = 512 * 1024 * 1024
)

// Config is the configuration for cloud storage sink.
type Config struct {
	WorkerCount              int
	FlushInterval            time.Duration
	FileSize                 int
	FileIndexWidth           int
	DateSeparator            string
	EnablePartitionSeparator bool
}

// NewConfig returns the default cloud storage sink config.
func NewConfig() *Config {
	return &Config{
		WorkerCount:   defaultWorkerCount,
		FlushInterval: defaultFlushInterval,
		FileSize:      defaultFileSize,
	}
}

// Apply applies the sink URI parameters to the config.
func (c *Config) Apply(
	ctx context.Context,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
) (err error) {
	if sinkURI == nil {
		return cerror.ErrStorageSinkInvalidConfig.GenWithStack(
			"failed to open cloud storage sink, empty SinkURI")
	}

	scheme := strings.ToLower(sinkURI.Scheme)
	if !psink.IsStorageScheme(scheme) {
		return cerror.ErrStorageSinkInvalidConfig.GenWithStack(
			"can't create cloud storage sink with unsupported scheme: %s", scheme)
	}
	query := sinkURI.Query()
	if err = getWorkerCount(query, &c.WorkerCount); err != nil {
		return err
	}
	err = getFlushInterval(query, &c.FlushInterval)
	if err != nil {
		return err
	}
	err = getFileSize(query, &c.FileSize)
	if err != nil {
		return err
	}

	c.DateSeparator = replicaConfig.Sink.DateSeparator
	c.EnablePartitionSeparator = replicaConfig.Sink.EnablePartitionSeparator
	c.FileIndexWidth = replicaConfig.Sink.FileIndexWidth

	if c.FileIndexWidth < config.MinFileIndexWidth || c.FileIndexWidth > config.MaxFileIndexWidth {
		c.FileIndexWidth = config.DefaultFileIndexWidth
	}

	return nil
}

func getWorkerCount(values url.Values, workerCount *int) error {
	s := values.Get("worker-count")
	if len(s) == 0 {
		return nil
	}

	c, err := strconv.Atoi(s)
	if err != nil {
		return cerror.WrapError(cerror.ErrStorageSinkInvalidConfig, err)
	}
	if c <= 0 {
		return cerror.WrapError(cerror.ErrStorageSinkInvalidConfig,
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
		return cerror.WrapError(cerror.ErrStorageSinkInvalidConfig, err)
	}

	if d > maxFlushInterval {
		log.Warn("flush-interval is too large", zap.Duration("original", d),
			zap.Duration("override", maxFlushInterval))
		d = maxFlushInterval
	}
	if d < minFlushInterval {
		log.Warn("flush-interval is too small", zap.Duration("original", d),
			zap.Duration("override", minFlushInterval))
		d = minFlushInterval
	}

	*flushInterval = d
	return nil
}

func getFileSize(values url.Values, fileSize *int) error {
	s := values.Get("file-size")
	if len(s) == 0 {
		return nil
	}

	sz, err := strconv.Atoi(s)
	if err != nil {
		return cerror.WrapError(cerror.ErrStorageSinkInvalidConfig, err)
	}
	if sz > maxFileSize {
		log.Warn("file-size is too large",
			zap.Int("original", sz), zap.Int("override", maxFileSize))
		sz = maxFileSize
	}
	if sz < minFileSize {
		log.Warn("file-size is too small",
			zap.Int("original", sz), zap.Int("override", minFileSize))
		sz = minFileSize
	}
	*fileSize = sz
	return nil
}
