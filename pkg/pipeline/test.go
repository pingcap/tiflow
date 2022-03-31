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
	"github.com/pingcap/tiflow/pkg/context"
	pmessage "github.com/pingcap/tiflow/pkg/pipeline/message"
)

// SendMessageToNode4Test sends messages to specified `Node` through `Receive` in order.
// This function is only for testing.
// Only `Node.Receive` will be called, other functions in `Node` will never be called.
// When the `Receive` function of the `Node` returns an error, this function will return the message that caused the error and the error
func SendMessageToNode4Test(
	ctx context.Context, node Node, msgs []pmessage.Message, outputCh chan pmessage.Message,
) (pmessage.Message, error) {
	nodeCtx := NewNodeContext(ctx, pmessage.Message{}, outputCh)
	for _, msg := range msgs {
		err := node.Receive(withMessage(nodeCtx, msg))
		if err != nil {
			return msg, err
		}
	}
	return pmessage.Message{}, nil
}

// MockNodeContext4Test creates a node context with a message and an output channel for tests.
func MockNodeContext4Test(
	ctx context.Context, msg pmessage.Message, outputCh chan pmessage.Message,
) NodeContext {
	return NewNodeContext(ctx, msg, outputCh)
}
