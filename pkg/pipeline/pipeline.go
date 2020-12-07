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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

const defaultOutputChannelSize = 1280000

// Pipeline represents a pipeline includes a number of nodes
type Pipeline struct {
	header    headRunner
	runners   []runner
	runnersWg sync.WaitGroup
	errors    []error
	errorsMu  sync.Mutex
	closeMu   sync.Mutex
	isClosed  bool
}

// NewPipeline creates a new pipeline
func NewPipeline(ctx context.Context) (context.Context, *Pipeline) {
	header := make(headRunner, 4)
	runners := make([]runner, 0, 16)
	runners = append(runners, header)
	p := &Pipeline{
		header:  header,
		runners: runners,
	}
	ctx = context.WatchThrow(ctx, func(err error) {
		p.addError(err)
		p.close()
	})
	go func() {
		<-ctx.Done()
		p.close()
	}()
	return ctx, p
}

// AppendNode appends the node to the pipeline
func (p *Pipeline) AppendNode(ctx context.Context, name string, node Node) {
	lastRunner := p.runners[len(p.runners)-1]
	runner := newNodeRunner(name, node, lastRunner)
	p.runners = append(p.runners, runner)
	p.runnersWg.Add(1)
	go p.driveRunner(ctx, lastRunner, runner)
}

func (p *Pipeline) driveRunner(ctx context.Context, previousRunner, runner runner) {
	defer p.runnersWg.Done()
	defer blackhole(previousRunner)
	err := runner.run(ctx)
	if err != nil {
		p.close()
		p.addError(err)
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
	defer func() {
		// Avoid panic because repeated close channel
		recover() //nolint:errcheck
	}()
	p.closeMu.Lock()
	defer p.closeMu.Unlock()
	p.isClosed = true
	close(p.header)
}

func (p *Pipeline) addError(err error) {
	p.errorsMu.Lock()
	defer p.errorsMu.Unlock()
	p.errors = append(p.errors, err)
}

// Wait all the nodes exited and return the errors found from nodes
func (p *Pipeline) Wait() []error {
	p.runnersWg.Wait()
	p.errorsMu.Lock()
	defer p.errorsMu.Unlock()
	return p.errors
}
