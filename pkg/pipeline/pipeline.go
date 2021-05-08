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

package pipeline

import (
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

// TODO: processor output chan size, the accumulated data is determined by
// the count of sorted data and unmounted data. In current benchmark a single
// processor can reach 50k-100k QPS, and accumulated data is around
// 200k-400k in most cases. We need a better chan cache mechanism.
const defaultOutputChannelSize = 512

// Pipeline represents a pipeline includes a number of nodes
type Pipeline struct {
	header    headRunner
	runners   []runner
	runnersWg sync.WaitGroup
	closeMu   sync.Mutex
	isClosed  bool
}

// NewPipeline creates a new pipeline
func NewPipeline(ctx context.Context, tickDuration time.Duration) *Pipeline {
	header := make(headRunner, 4)
	runners := make([]runner, 0, 16)
	runners = append(runners, header)
	p := &Pipeline{
		header:  header,
		runners: runners,
	}
	go func() {
		var tickCh <-chan time.Time
		if tickDuration > 0 {
			ticker := time.NewTicker(tickDuration)
			defer ticker.Stop()
			tickCh = ticker.C
		} else {
			tickCh = make(chan time.Time)
		}
		for {
			select {
			case <-tickCh:
				p.SendToFirstNode(TickMessage()) //nolint:errcheck
			case <-ctx.Done():
				p.close()
				return
			}
		}
	}()
	return p
}

// AppendNode appends the node to the pipeline
func (p *Pipeline) AppendNode(ctx context.Context, name string, node Node) {
	ctx = context.WithErrorHandler(ctx, func(err error) error {
		p.close()
		return err
	})
	lastRunner := p.runners[len(p.runners)-1]
	runner := newNodeRunner(name, node, lastRunner)
	p.runners = append(p.runners, runner)
	p.runnersWg.Add(1)
	go p.driveRunner(ctx, lastRunner, runner)
}

func (p *Pipeline) driveRunner(ctx context.Context, previousRunner, runner runner) {
	defer func() {
		log.Info("a pipeline node is exiting, stop the whole pipeline", zap.String("name", runner.getName()))
		p.close()
		blackhole(previousRunner)
		p.runnersWg.Done()
	}()
	err := runner.run(ctx)
	if err != nil {
		ctx.Throw(err)
		log.Error("found error when running the node", zap.String("name", runner.getName()), zap.Error(err))
	}
}

// SendToFirstNode sends the message to the first node
func (p *Pipeline) SendToFirstNode(msg *Message) error {
	p.closeMu.Lock()
	defer p.closeMu.Unlock()
	if p.isClosed {
		return cerror.ErrSendToClosedPipeline.GenWithStackByArgs()
	}
	// The header channel should never be blocked
	p.header <- msg
	return nil
}

func (p *Pipeline) close() {
	p.closeMu.Lock()
	defer p.closeMu.Unlock()
	if !p.isClosed {
		close(p.header)
		p.isClosed = true
	}
}

// Wait all the nodes exited
func (p *Pipeline) Wait() {
	p.runnersWg.Wait()
}
