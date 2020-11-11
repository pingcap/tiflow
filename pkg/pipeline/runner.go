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
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type runner interface {
	run(ctx Context) error
	getOutputCh() chan *Message
	getNode() Node
	getName() string
}

type nodeRunner struct {
	name     string
	node     Node
	previous runner
	outputCh chan *Message
}

func newNodeRunner(name string, node Node, previous runner) *nodeRunner {
	return &nodeRunner{
		name:     name,
		node:     node,
		previous: previous,
		outputCh: make(chan *Message, defaultOutputChannelSize),
	}
}

func (r *nodeRunner) run(ctx Context) error {
	ctx = withOutputCh(ctx, r.outputCh)
	defer func() {
		err := r.node.Destroy(ctx)
		if err != nil {
			log.Error("found an error when stopping node", zap.String("node name", r.name), zap.Error(err))
		}
		close(r.outputCh)
	}()
	err := r.node.Init(ctx)
	if err != nil {
		return err
	}
	// TODO: We can add monitoring for execution time and channel length here uniformly
	for msg := range r.previous.getOutputCh() {
		err := r.node.Receive(withMessage(ctx, msg))
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *nodeRunner) getOutputCh() chan *Message {
	return r.outputCh
}

func (r *nodeRunner) getNode() Node {
	return r.node
}

func (r *nodeRunner) getName() string {
	return r.name
}

type headRunner chan *Message

func (h headRunner) getName() string {
	return "header"
}

func (h headRunner) run(ctx Context) error {
	panic("unreachable")
}

func (h headRunner) getOutputCh() chan *Message {
	return h
}

func (h headRunner) getNode() Node {
	panic("unreachable")
}

func blackhole(runner runner) {
	for range runner.getOutputCh() {
		// ignore all messages in the outputCh of the runner
	}
}
