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

package util

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// MonitorCancelLatency monitors the latency from ctx being cancelled
// the first returned function should be called when the cancellation is done
// the second returned function should be called to mark the cancellation is started, it will start a
// background go routine to monitor the latency util finish is called or cancellation is done
func MonitorCancelLatency(ctx context.Context, identifier string) (func(), func()) {
	finishedCh := make(chan struct{})
	start := func() {
		go func() {
			log.Debug("MonitorCancelLatency: Cancelled", zap.String("identifier", identifier))
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			elapsed := 0
			for {
				select {
				case <-finishedCh:
					log.Debug("MonitorCancelLatency: Monitored routine exited", zap.String("identifier", identifier))
					return
				case <-ticker.C:
					elapsed++
					log.Warn("MonitorCancelLatency: Cancellation is taking too long",
						zap.String("identifier", identifier),
						zap.Int("duration", elapsed), zap.Error(ctx.Err()))
				}
			}
		}()
	}
	return func() {
		close(finishedCh)
	}, start
}
