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

package master

import (
	"context"
	"math"

	"github.com/pingcap/dm/pkg/log"

	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// rate limit related constant value.
const (
	DefaultRate      = float64(10)
	DefaultBurst     = 40
	ErrorNoEmitToken = "fail to get emit opportunity for %s"
)

type emitFunc func(args ...interface{})

// AgentPool is a pool to control communication with dm-workers
// It provides rate limit control for agent acquire, including dispatch rate r
// and permits bursts of at most b tokens.
// caller shouldn't to hold agent to avoid deadlock.
type AgentPool struct {
	requests chan int
	agents   chan *Agent
	cfg      *RateLimitConfig
	limiter  *rate.Limiter
}

// RateLimitConfig holds rate limit config.
type RateLimitConfig struct {
	rate  float64 // dispatch rate
	burst int     // max permits bursts
}

// Agent communicate with dm-workers.
type Agent struct {
	ID int
}

// NewAgentPool returns a agent pool.
func NewAgentPool(cfg *RateLimitConfig) *AgentPool {
	requests := make(chan int, int(math.Ceil(1/cfg.rate))+cfg.burst)
	agents := make(chan *Agent, cfg.burst)
	limiter := rate.NewLimiter(rate.Limit(cfg.rate), cfg.burst)

	return &AgentPool{
		requests: requests,
		agents:   agents,
		cfg:      cfg,
		limiter:  limiter,
	}
}

// Apply applies for a agent
// if ctx is canceled before we get an agent, returns nil.
func (ap *AgentPool) Apply(ctx context.Context, id int) *Agent {
	select {
	case <-ctx.Done():
		return nil
	case ap.requests <- id:
	}

	select {
	case <-ctx.Done():
		return nil
	case agent := <-ap.agents:
		return agent
	}
}

// Start starts AgentPool background dispatcher.
func (ap *AgentPool) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case id := <-ap.requests:
			err := ap.limiter.Wait(ctx)
			if err != nil {
				if err != context.Canceled {
					log.L().Fatal("agent limiter wait meets unexpected error", zap.Error(err))
				}
				return
			}
			select {
			case <-ctx.Done():
				return
			case ap.agents <- &Agent{ID: id}:
			}
		}
	}
}

// Emit applies for an agent to communicates with dm-worker.
func (ap *AgentPool) Emit(ctx context.Context, id int, fn emitFunc, errFn emitFunc, args ...interface{}) {
	if agent := ap.Apply(ctx, id); agent == nil {
		errFn(args...)
	} else {
		fn(args...)
	}
}
