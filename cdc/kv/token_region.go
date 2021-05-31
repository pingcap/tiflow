// Copyright 2021 PingCAP, Inc.
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

package kv

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	regionOutputChanSize = 16
)

// LimitRegionRouter defines an interface that can buffer singleRegionInfo
// and provide token based consumption
type LimitRegionRouter interface {
	// Chan returns a singleRegionInfo channel that can be consumed from
	Chan() <-chan singleRegionInfo
	// AddRegion adds an singleRegionInfo to buffer
	AddRegion(task singleRegionInfo)
	// RevokeToken gives back one token
	RevokeToken()
	// Run runs in background and does some logic work
	Run(ctx context.Context) error
}

type sizedRegionRouter struct {
	buffer          []singleRegionInfo
	output          chan singleRegionInfo
	lock            sync.Mutex
	metricTokenSize prometheus.Gauge
	tokenUsed       int
	sizeLimit       int
}

// NewSizedRegionRouter creates a new sizedRegionRouter
func NewSizedRegionRouter(sizeLimit int, metric prometheus.Gauge) *sizedRegionRouter {
	return &sizedRegionRouter{
		buffer:          make([]singleRegionInfo, 0),
		output:          make(chan singleRegionInfo, regionOutputChanSize),
		sizeLimit:       sizeLimit,
		metricTokenSize: metric,
	}
}

func (r *sizedRegionRouter) Chan() <-chan singleRegionInfo {
	return r.output
}

func (r *sizedRegionRouter) AddRegion(sri singleRegionInfo) {
	r.lock.Lock()
	if r.sizeLimit > r.tokenUsed && len(r.output) < regionOutputChanSize {
		r.output <- sri
		r.tokenUsed++
		r.metricTokenSize.Inc()
	} else {
		r.buffer = append(r.buffer, sri)
	}
	r.lock.Unlock()
}

func (r *sizedRegionRouter) RevokeToken() {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.tokenUsed--
	r.metricTokenSize.Dec()
}

func (r *sizedRegionRouter) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			r.lock.Lock()
			available := r.sizeLimit - r.tokenUsed
			if available > len(r.buffer) {
				available = len(r.buffer)
			}
			// to avoid deadlock because when consuming from the output channel.
			// onRegionFail could decrease tokenUsed, which requires lock protection.
			if available > regionOutputChanSize-len(r.output) {
				available = regionOutputChanSize - len(r.output)
			}
			if available == 0 {
				r.lock.Unlock()
				continue
			}
			for i := 0; i < available; i++ {
				select {
				case <-ctx.Done():
					r.lock.Unlock()
					return errors.Trace(ctx.Err())
				case r.output <- r.buffer[i]:
				}
			}
			r.buffer = r.buffer[available:]
			r.tokenUsed += available
			r.metricTokenSize.Add(float64(available))
			r.lock.Unlock()
		}
	}
}
