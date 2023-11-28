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
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gin-gonic/gin/binding"
	"github.com/imdario/mergo"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	psink "github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/util"
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

	// disable file cleanup by default
	defaultFileExpirationDays = 0
	// Second | Minute | Hour | Dom | Month | DowOptional
	// `0 0 2 * * ?` means 2:00:00 AM every day
	defaultFileCleanupCronSpec = "0 0 2 * * *"
)

type urlConfig struct {
	WorkerCount   *int    `form:"worker-count"`
	FlushInterval *string `form:"flush-interval"`
	FileSize      *int    `form:"file-size"`
}

// Config is the configuration for cloud storage sink.
type Config struct {
	WorkerCount              int
	FlushInterval            time.Duration
	FileSize                 int
	FileIndexWidth           int
	DateSeparator            string
	FileExpirationDays       int
	FileCleanupCronSpec      string
	EnablePartitionSeparator bool
	OutputColumnID           bool
}

// NewConfig returns the default cloud storage sink config.
func NewConfig() *Config {
	return &Config{
		WorkerCount:         defaultWorkerCount,
		FlushInterval:       defaultFlushInterval,
		FileSize:            defaultFileSize,
		FileExpirationDays:  defaultFileExpirationDays,
		FileCleanupCronSpec: defaultFileCleanupCronSpec,
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
	req := &http.Request{URL: sinkURI}
	urlParameter := &urlConfig{}
	if err := binding.Query.Bind(req, urlParameter); err != nil {
		return cerror.WrapError(cerror.ErrStorageSinkInvalidConfig, err)
	}
	if urlParameter, err = mergeConfig(replicaConfig, urlParameter); err != nil {
		return err
	}
	if err = getWorkerCount(urlParameter, &c.WorkerCount); err != nil {
		return err
	}
	err = getFlushInterval(urlParameter, &c.FlushInterval)
	if err != nil {
		return err
	}
	err = getFileSize(urlParameter, &c.FileSize)
	if err != nil {
		return err
	}

	c.DateSeparator = util.GetOrZero(replicaConfig.Sink.DateSeparator)
	c.EnablePartitionSeparator = util.GetOrZero(replicaConfig.Sink.EnablePartitionSeparator)
	c.FileIndexWidth = util.GetOrZero(replicaConfig.Sink.FileIndexWidth)
	if replicaConfig.Sink.CloudStorageConfig != nil {
		c.OutputColumnID = util.GetOrZero(replicaConfig.Sink.CloudStorageConfig.OutputColumnID)
		if replicaConfig.Sink.CloudStorageConfig.FileExpirationDays != nil {
			c.FileExpirationDays = *replicaConfig.Sink.CloudStorageConfig.FileExpirationDays
		}
		if replicaConfig.Sink.CloudStorageConfig.FileCleanupCronSpec != nil {
			c.FileCleanupCronSpec = *replicaConfig.Sink.CloudStorageConfig.FileCleanupCronSpec
		}
	}

	if c.FileIndexWidth < config.MinFileIndexWidth || c.FileIndexWidth > config.MaxFileIndexWidth {
		c.FileIndexWidth = config.DefaultFileIndexWidth
	}

	return nil
}

func mergeConfig(
	replicaConfig *config.ReplicaConfig,
	urlParameters *urlConfig,
) (*urlConfig, error) {
	dest := &urlConfig{}
	if replicaConfig.Sink != nil && replicaConfig.Sink.CloudStorageConfig != nil {
		dest.WorkerCount = replicaConfig.Sink.CloudStorageConfig.WorkerCount
		dest.FlushInterval = replicaConfig.Sink.CloudStorageConfig.FlushInterval
		dest.FileSize = replicaConfig.Sink.CloudStorageConfig.FileSize
	}
	if err := mergo.Merge(dest, urlParameters, mergo.WithOverride); err != nil {
		return nil, cerror.WrapError(cerror.ErrStorageSinkInvalidConfig, err)
	}
	return dest, nil
}

func getWorkerCount(values *urlConfig, workerCount *int) error {
	if values.WorkerCount == nil {
		return nil
	}

	c := *values.WorkerCount
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

func getFlushInterval(values *urlConfig, flushInterval *time.Duration) error {
	if values.FlushInterval == nil || len(*values.FlushInterval) == 0 {
		return nil
	}

	d, err := time.ParseDuration(*values.FlushInterval)
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

func getFileSize(values *urlConfig, fileSize *int) error {
	if values.FileSize == nil {
		return nil
	}

	sz := *values.FileSize
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
