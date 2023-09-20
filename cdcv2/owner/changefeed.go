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

package owner

import (
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor"
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/cdcv2/metadata"
	"github.com/pingcap/tiflow/cdcv2/metadata/memory"
	"go.uber.org/zap"
)

type changefeedImpl struct {
	scheduler scheduler.Scheduler

	Info      *model.ChangeFeedInfo
	Status    *model.ChangeFeedStatus
	owner     metadata.OwnerObservation
	storage   *memory.Storage
	processor processor.Manager
}

func newChangefeed(storage *memory.Storage,
	c *metadata.ChangefeedInfo,
	id metadata.ChangefeedIDWithEpoch) *changefeedImpl {
	memory.NewOwnerDB(storage, c, id)
	return &changefeedImpl{}
}

// GetInfoProvider returns an InfoProvider if one is available.
func (c *changefeedImpl) GetInfoProvider() scheduler.InfoProvider {
	if provider, ok := c.scheduler.(scheduler.InfoProvider); ok {
		return provider
	}
	return nil
}

func (c *changefeedImpl) tick(ctx context.Context, captures map[model.CaptureID]*model.CaptureInfo) error {
	return nil
}

func (c *changefeedImpl) Close(ctx context.Context) {
	c.releaseResources(ctx)
}

func (c *changefeedImpl) releaseResources(ctx context.Context) {
	log.Info("changefeed closed",
		zap.String("namespace", c.Info.Namespace),
		zap.String("changefeed", c.Info.ID))
}
