// Copyright 2019 PingCAP, Inc.
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

package cdc

import (
	"context"

	"github.com/pingcap/ticdc/cdc/sink"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

// runProcessor creates a new processor then starts it.
func runProcessor(
	ctx context.Context,
	pdEndpoints []string,
	info model.ChangeFeedInfo,
	changefeedID string,
	captureID string,
	checkpointTs uint64,
) (*processor, error) {
	opts := make(map[string]string, len(info.Opts)+2)
	for k, v := range info.Opts {
		opts[k] = v
	}
	opts[sink.OptChangefeedID] = changefeedID
	opts[sink.OptCaptureID] = captureID
	filter, err := util.NewFilter(info.GetConfig())
	if err != nil {
		return nil, errors.Trace(err)
	}
	sink, err := sink.NewSink(info.SinkURI, filter, opts)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ctx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		if err := sink.Run(ctx); errors.Cause(err) != context.Canceled {
			errCh <- err
		}
	}()
	processor, err := NewProcessor(ctx, pdEndpoints, info, sink, changefeedID, captureID, checkpointTs)
	if err != nil {
		cancel()
		return nil, err
	}
	log.Info("start to run processor", zap.String("changefeed id", changefeedID))

	processor.Run(ctx, errCh)

	go func() {
		err := <-errCh
		if errors.Cause(err) != context.Canceled {
			log.Error("error on running processor",
				zap.String("captureid", captureID),
				zap.String("changefeedid", changefeedID),
				zap.String("processorid", processor.id),
				zap.Error(err))
		} else {
			log.Info("processor exited",
				zap.String("captureid", captureID),
				zap.String("changefeedid", changefeedID),
				zap.String("processorid", processor.id))
		}
		cancel()
	}()

	return processor, nil
}
