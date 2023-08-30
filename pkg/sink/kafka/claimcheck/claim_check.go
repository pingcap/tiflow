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
	"path/filepath"
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
	"golang.org/x/sync/errgroup"
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
	externalStorage, err := util.GetExternalStorageFromURI(ctx, storageURI)
	if err != nil {
		return nil, errors.Trace(err)
	}

	log.Info("claim-check enabled",
		zap.String("namespace", changefeedID.Namespace),
		zap.String("changefeed", changefeedID.ID),
		zap.String("storageURI", storageURI))

	return &ClaimCheck{
		changefeedID:              changefeedID,
		storage:                   externalStorage,
		metricSendMessageDuration: claimCheckSendMessageDuration.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		metricSendMessageCount:    claimCheckSendMessageCount.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
	}, nil
}

// WriteMessage write message to the claim check external storage.
func (c *ClaimCheck) WriteMessage(ctx context.Context, messages ...*common.Message) error {
	if len(messages) == 0 {
		return nil
	}

	g, ctx := errgroup.WithContext(ctx)
	start := time.Now()
	for idx := 0; idx < len(messages); idx++ {
		message := messages[idx]
		g.Go(func() error {
			m := common.ClaimCheckMessage{
				Key:   message.Key,
				Value: message.Value,
			}
			data, err := json.Marshal(m)
			if err != nil {
				return errors.Trace(err)
			}
			err = c.storage.WriteFile(ctx, message.ClaimCheckFileName, data)
			if err != nil {
				return errors.Trace(err)
			}
			return nil
		})
	}
	err := g.Wait()
	if err != nil {
		return errors.Trace(err)
	}
	c.metricSendMessageDuration.Observe(time.Since(start).Seconds())
	c.metricSendMessageCount.Add(float64(len(messages)))

	log.Info("claim-check batch write messages", zap.Int("count", len(messages)))

	return nil
}

// Close the claim check by clean up the metrics.
func (c *ClaimCheck) Close() {
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

// FileNameWithPrefix returns the file name with prefix, the full path.
func FileNameWithPrefix(prefix, fileName string) string {
	return filepath.Join(prefix, fileName)
}
