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
	Chan(sessionId string) <-chan singleRegionInfo
	// AddRegion adds an singleRegionInfo to buffer, this function is thread-safe
	AddRegion(sessionId string, task singleRegionInfo)
	// Acquire acquires one token
	Acquire(sessionId string, id string)
	// Release gives back one token, this function is thread-safe
	Release(sessionId string, id string)
	// Run runs in background and does some logic work
	Run(ctx context.Context) error
	AddSession(sessionId string)
	RemoveSession(sessionId string)
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

func newSrrMetrics(captureAddr, changefeed string) *srrMetrics {
	return &srrMetrics{
		capture:       captureAddr,
		changefeed:    changefeed,
		tokens:        make(map[string]prometheus.Gauge),
		cachedRegions: make(map[string]prometheus.Gauge),
	}
}

// each changefeed on a capture maintains a sizedRegionRouter
type sizedRegionRouter struct {
	buffer     map[string]map[string][]singleRegionInfo
	output     map[string]chan singleRegionInfo
	lock       sync.Mutex
	metrics    map[string]*srrMetrics
	tokens     map[string]map[string]int
	sizeLimit  int
	capture    string
	changefeed string
}

// NewSizedRegionRouter creates a new sizedRegionRouter
func NewSizedRegionRouter(ctx context.Context, sizeLimit int) *sizedRegionRouter {
	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeed := util.ChangefeedIDFromCtx(ctx)
	return &sizedRegionRouter{
		buffer:     make(map[string]map[string][]singleRegionInfo),
		output:     make(map[string]chan singleRegionInfo, regionRouterChanSize),
		sizeLimit:  sizeLimit,
		tokens:     make(map[string]map[string]int),
		metrics:    make(map[string]*srrMetrics),
		capture:    captureAddr,
		changefeed: changefeed,
	}
}

func (r *sizedRegionRouter) Chan(sessionId string) <-chan singleRegionInfo {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.output[sessionId]
}

func (r *sizedRegionRouter) AddRegion(sessionId string, sri singleRegionInfo) {
	r.lock.Lock()
	var id string
	// if rpcCtx is not provided, use the default "" bucket
	if sri.rpcCtx != nil {
		id = sri.rpcCtx.Addr
	}
	if r.sizeLimit > r.tokens[sessionId][id] && len(r.output) < regionRouterChanSize {
		r.output[sessionId] <- sri
	} else {
		r.buffer[sessionId][id] = append(r.buffer[sessionId][id], sri)
		if _, ok := r.metrics[sessionId].cachedRegions[id]; !ok {
			r.metrics[sessionId].cachedRegions[id] = cachedRegionSize.WithLabelValues(id, r.metrics[sessionId].changefeed, r.metrics[sessionId].capture)
		}
		r.metrics[sessionId].cachedRegions[id].Inc()
	}
	r.lock.Unlock()
}

// Acquire implements LimitRegionRouter.Acquire
// param: id is TiKV store address
func (r *sizedRegionRouter) Acquire(sessionId string, id string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.tokens[sessionId][id]++
	if _, ok := r.metrics[sessionId].tokens[id]; !ok {
		r.metrics[sessionId].tokens[id] = clientRegionTokenSize.WithLabelValues(id, r.metrics[sessionId].changefeed, r.metrics[sessionId].capture)
	}
	r.metrics[sessionId].tokens[id].Inc()
}

// Release implements LimitRegionRouter.Release
// param: id is TiKV store address
func (r *sizedRegionRouter) Release(sessionId string, id string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.tokens[sessionId][id]--
	if _, ok := r.metrics[sessionId].tokens[id]; !ok {
		r.metrics[sessionId].tokens[id] = clientRegionTokenSize.WithLabelValues(id, r.metrics[sessionId].changefeed, r.metrics[sessionId].capture)
	}
	r.metrics[sessionId].tokens[id].Dec()
}

func (r *sizedRegionRouter) AddSession(sessionId string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.buffer[sessionId] = make(map[string][]singleRegionInfo)
	r.tokens[sessionId] = make(map[string]int)
	r.output[sessionId] = make(chan singleRegionInfo, regionRouterChanSize)
	r.metrics[sessionId] = newSrrMetrics(r.capture, r.changefeed)
}

func (r *sizedRegionRouter) RemoveSession(sessionId string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	delete(r.buffer, sessionId)
	delete(r.tokens, sessionId)
	delete(r.output, sessionId)
	delete(r.metrics, sessionId)
}

func (r *sizedRegionRouter) Run(ctx context.Context) error {
	ticker := time.NewTicker(sizedRegionCheckInterval)
	defer func() {
		ticker.Stop()
		r.lock.Lock()
		defer r.lock.Unlock()
		for session := range r.buffer {
			for id, buf := range r.buffer[session] {
				r.metrics[session].cachedRegions[id].Sub(float64(len(buf)))
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			r.lock.Lock()
			for session := range r.buffer {
				for id, buf := range r.buffer[session] {
					available := r.sizeLimit - r.tokens[session][id]
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
						case r.output[session] <- buf[i]:
						}
					}
					r.buffer[session][id] = r.buffer[session][id][available:]
					r.metrics[session].cachedRegions[id].Sub(float64(available))
				}
			}
			r.lock.Unlock()
		}
	}
}
