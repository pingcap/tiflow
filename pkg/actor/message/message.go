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

package message

import (
	"github.com/pingcap/tiflow/cdc/model"
	sorter "github.com/pingcap/tiflow/cdc/sorter/leveldb/message"
)

// Type is the type of Message
type Type int

// types of Message
const (
	TypeUnknown Type = iota
	TypeTick
	TypeStop
	TypeBarrier
	TypeSorterTask
	// Add a new type when adding a new message.
)

// Message is a vehicle for transferring information between nodes
type Message struct {
	// Tp is the type of Message
	Tp Type
	// BarrierTs
	BarrierTs model.Ts
	// Leveldb sorter task
	// TODO: find a way to hide it behind an interface while saving
	//       memory allocation.
	// See https://cs.opensource.google/go/go/+/refs/tags/go1.17.2:src/runtime/iface.go;l=325
	SorterTask sorter.Task
}

// TickMessage creates the message of Tick
func TickMessage() Message {
	return Message{
		Tp: TypeTick,
	}
}

// StopMessage creates the message of Stop.
// After receiving a Stop message, actor will be closed.
func StopMessage() Message {
	return Message{
		Tp: TypeStop,
	}
}

// BarrierMessage creates the message of Command
func BarrierMessage(barrierTs model.Ts) Message {
	return Message{
		Tp:        TypeBarrier,
		BarrierTs: barrierTs,
	}
}

// SorterMessage creates the message of sorter
func SorterMessage(task sorter.Task) Message {
	return Message{
		Tp:         TypeSorterTask,
		SorterTask: task,
	}
}
