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
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// buffer size for ranged region consumer
	regionRouterChanSize = 16
	// sizedRegionRouter checks region buffer every 100ms
	sizedRegionCheckInterval = 100 * time.Millisecond
)

// LimitRegionRouter defines an interface that can buffer singleRegionInfo
// and provide token based consumption
type LimitRegionRouter interface {
	// Chan returns a singleRegionInfo channel that can be consumed from
	Chan() <-chan singleRegionInfo
	// AddRegion adds an singleRegionInfo to buffer, this function is thread-safe
	AddRegion(task singleRegionInfo)
	// Acquire acquires one token
	Acquire(id string)
	// Release gives back one token, this function is thread-safe
	Release(id string)
	// Run runs in background and does some logic work
	Run(ctx context.Context) error
}

// srrMetrics keeps metrics of a Sized Region Router
type srrMetrics struct {
	capture    string
	changefeed string
	// mapping from id(TiKV store address) to token used
	tokens map[string]prometheus.Gauge
	// mapping from id(TiKV store address) to cached regions
	cachedRegions map[string]prometheus.Gauge
}

func newSrrMetrics(ctx context.Context) *srrMetrics {
	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeed := util.ChangefeedIDFromCtx(ctx)
	return &srrMetrics{
		capture:       captureAddr,
		changefeed:    changefeed,
		tokens:        make(map[string]prometheus.Gauge),
		cachedRegions: make(map[string]prometheus.Gauge),
	}
}

// each changefeed on a capture maintains a sizedRegionRouter
type sizedRegionRouter struct {
	buffer    map[string][]singleRegionInfo
	output    chan singleRegionInfo
	lock      sync.Mutex
	metrics   *srrMetrics
	tokens    map[string]int
	sizeLimit int
}

// NewSizedRegionRouter creates a new sizedRegionRouter
func NewSizedRegionRouter(ctx context.Context, sizeLimit int) *sizedRegionRouter {
	return &sizedRegionRouter{
		buffer:    make(map[string][]singleRegionInfo),
		output:    make(chan singleRegionInfo, regionRouterChanSize),
		sizeLimit: sizeLimit,
		tokens:    make(map[string]int),
		metrics:   newSrrMetrics(ctx),
	}
}

func (r *sizedRegionRouter) Chan() <-chan singleRegionInfo {
	return r.output
}

func (r *sizedRegionRouter) AddRegion(sri singleRegionInfo) {
	r.lock.Lock()
	var id string
	// if rpcCtx is not provided, use the default "" bucket
	if sri.rpcCtx != nil {
		id = sri.rpcCtx.Addr
	}
	if r.sizeLimit > r.tokens[id] && len(r.output) < regionRouterChanSize {
		r.output <- sri
	} else {
		r.buffer[id] = append(r.buffer[id], sri)
		if _, ok := r.metrics.cachedRegions[id]; !ok {
			r.metrics.cachedRegions[id] = cachedRegionSize.WithLabelValues(id, r.metrics.changefeed, r.metrics.capture)
		}
		r.metrics.cachedRegions[id].Inc()
	}
	r.lock.Unlock()
}

// Acquire implements LimitRegionRouter.Acquire
// param: id is TiKV store address
func (r *sizedRegionRouter) Acquire(id string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.tokens[id]++
	if _, ok := r.metrics.tokens[id]; !ok {
		r.metrics.tokens[id] = clientRegionTokenSize.WithLabelValues(id, r.metrics.changefeed, r.metrics.capture)
	}
	r.metrics.tokens[id].Inc()
}

// Release implements LimitRegionRouter.Release
// param: id is TiKV store address
func (r *sizedRegionRouter) Release(id string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.tokens[id]--
	if _, ok := r.metrics.tokens[id]; !ok {
		r.metrics.tokens[id] = clientRegionTokenSize.WithLabelValues(id, r.metrics.changefeed, r.metrics.capture)
	}
	r.metrics.tokens[id].Dec()
}

func (r *sizedRegionRouter) Run(ctx context.Context) error {
	ticker := time.NewTicker(sizedRegionCheckInterval)
	defer func() {
		ticker.Stop()
		r.lock.Lock()
		defer r.lock.Unlock()
		for id, buf := range r.buffer {
			r.metrics.cachedRegions[id].Sub(float64(len(buf)))
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			r.lock.Lock()
			for id, buf := range r.buffer {
				available := r.sizeLimit - r.tokens[id]
				// the tokens used could be more than size limit, since we have
				// a sized channel as level1 cache
				if available <= 0 {
					continue
				}
				if available > len(buf) {
					available = len(buf)
				}
				// to avoid deadlock because when consuming from the output channel.
				// onRegionFail could decrease tokens, which requires lock protection.
				if available > regionRouterChanSize-len(r.output) {
					available = regionRouterChanSize - len(r.output)
				}
				if available == 0 {
					continue
				}
				for i := 0; i < available; i++ {
					select {
					case <-ctx.Done():
						r.lock.Unlock()
						return errors.Trace(ctx.Err())
					case r.output <- buf[i]:
					}
				}
				r.buffer[id] = r.buffer[id][available:]
				r.metrics.cachedRegions[id].Sub(float64(available))
			}
			r.lock.Unlock()
		}
	}
}
