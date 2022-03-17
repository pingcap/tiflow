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

// Node represents a handle unit for the message stream in the pipeline
// The following functions in this interface will be called in one goroutine.
// It's NO NEED to consider concurrency issues
type Node interface {
	// Init initializes the node
	// when the pipeline is started, this function will be called in order
	// you can call `ctx.SendToNextNode(msg)` to send the message to the next node
	// but it will return nil if you try to call the `ctx.Message()`
	Init(ctx NodeContext) error

	// Receive receives the message from the previous node
	// when the node receives a message, this function will be called
	// you can call `ctx.Message()` to receive the message
	// you can call `ctx.SendToNextNode(msg)` to send the message to the next node
	// You SHOULD NOT pass the ctx to another goroutine or store it pass the
	// lifetime of this function call.
	Receive(ctx NodeContext) error

	// Destroy frees the resources in this node
	// you can call `ctx.SendToNextNode(msg)` to send the message to the next node
	// but it will return nil if you try to call the `ctx.Message()`
	Destroy(ctx NodeContext) error
}
