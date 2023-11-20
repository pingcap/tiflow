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

package claimcheck

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	defaultTimeout = 5 * time.Minute
)

// ClaimCheck manage send message to the claim-check external storage.
type ClaimCheck struct {
	storage storage.ExternalStorage

	changefeedID model.ChangeFeedID

	// metricSendMessageDuration tracks the time duration
	// cost on send messages to the claim check external storage.
	metricSendMessageDuration prometheus.Observer
	metricSendMessageCount    prometheus.Counter
}

// New return a new ClaimCheck.
func New(ctx context.Context, storageURI string, changefeedID model.ChangeFeedID) (*ClaimCheck, error) {
	log.Info("claim check enabled, start create the external storage",
		zap.String("namespace", changefeedID.Namespace),
		zap.String("changefeed", changefeedID.ID),
		zap.String("storageURI", util.MaskSensitiveDataInURI(storageURI)))

	start := time.Now()
	externalStorage, err := util.GetExternalStorageWithTimeout(ctx, storageURI, defaultTimeout)
	if err != nil {
		log.Error("create external storage failed",
			zap.String("namespace", changefeedID.Namespace),
			zap.String("changefeed", changefeedID.ID),
			zap.String("storageURI", util.MaskSensitiveDataInURI(storageURI)),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err))
		return nil, errors.Trace(err)
	}

	log.Info("claim-check create the external storage success",
		zap.String("namespace", changefeedID.Namespace),
		zap.String("changefeed", changefeedID.ID),
		zap.String("storageURI", util.MaskSensitiveDataInURI(storageURI)),
		zap.Duration("duration", time.Since(start)))

	return &ClaimCheck{
		changefeedID:              changefeedID,
		storage:                   externalStorage,
		metricSendMessageDuration: claimCheckSendMessageDuration.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricSendMessageCount:    claimCheckSendMessageCount.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
	}, nil
}

// WriteMessage write message to the claim check external storage.
func (c *ClaimCheck) WriteMessage(ctx context.Context, key, value []byte, fileName string) error {
	m := common.ClaimCheckMessage{
		Key:   key,
		Value: value,
	}
	data, err := json.Marshal(m)
	if err != nil {
		return errors.Trace(err)
	}

	start := time.Now()
	err = c.storage.WriteFile(ctx, fileName, data)
	if err != nil {
		return errors.Trace(err)
	}
	c.metricSendMessageDuration.Observe(time.Since(start).Seconds())
	c.metricSendMessageCount.Inc()
	return nil
}

// FileNameWithPrefix returns the file name with prefix, the full path.
func (c *ClaimCheck) FileNameWithPrefix(fileName string) string {
	return strings.TrimSuffix(c.storage.URI(), "/") + "/" + fileName
}

// CleanMetrics the claim check by clean up the metrics.
func (c *ClaimCheck) CleanMetrics() {
	claimCheckSendMessageDuration.DeleteLabelValues(c.changefeedID.Namespace, c.changefeedID.ID)
	claimCheckSendMessageCount.DeleteLabelValues(c.changefeedID.Namespace, c.changefeedID.ID)
}

// NewFileName return the file name for the message which is delivered to the external storage system.
// UUID V4 is used to generate random and unique file names.
// This should not exceed the S3 object name length limit.
// ref https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
func NewFileName() string {
	return uuid.NewString() + ".json"
}
