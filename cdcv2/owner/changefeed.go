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
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/cdc/processor"
	"github.com/pingcap/tiflow/cdc/scheduler"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	"go.uber.org/zap"
)

type changefeedImpl struct {
	uuid uint64
	ID   model.ChangeFeedID

	Info       *model.ChangeFeedInfo
	Status     *model.ChangeFeedStatus
	processor  processor.Processor
	changefeed owner.Changefeed
}

func newChangefeed(changefeed owner.Changefeed,
	info *model.ChangeFeedInfo,
	status *model.ChangeFeedStatus,
	processor processor.Processor) *changefeedImpl {
	return &changefeedImpl{
		changefeed: changefeed,
		Status:     status,
		Info:       info,
		processor:  processor,
	}
}

// GetInfoProvider returns an InfoProvider if one is available.
func (c *changefeedImpl) GetInfoProvider() scheduler.InfoProvider {
	if provider, ok := c.changefeed.GetScheduler().(scheduler.InfoProvider); ok {
		return provider
	}
	return nil
}

func (c *changefeedImpl) Tick(ctx cdcContext.Context, info *model.ChangeFeedInfo,
	status *model.ChangeFeedStatus, captures map[model.CaptureID]*model.CaptureInfo) (model.Ts, model.Ts) {
	err, warning := c.processor.Tick(ctx, info, status)
	if err != nil {

	}
	if warning != nil {

	}
	return c.changefeed.Tick(ctx, info, status, captures)
}

func (c *changefeedImpl) Close(ctx cdcContext.Context) {
	c.releaseResources(ctx)
}

func (c *changefeedImpl) releaseResources(ctx context.Context) {
	log.Info("changefeed closed",
		zap.String("namespace", c.Info.Namespace),
		zap.String("changefeed", c.Info.ID))
}
