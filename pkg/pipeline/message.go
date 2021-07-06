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

import "github.com/pingcap/ticdc/cdc/model"

// MessageType is the type of Message
type MessageType int

// types of Message
const (
	MessageTypeUnknown MessageType = iota
	MessageTypeCommand
	MessageTypePolymorphicEvent
	MessageTypeBarrier
	MessageTypeTick
)

// Message is a vehicle for transferring information between nodes
type Message struct {
	// TODO add more kind of messages
	// Tp is the type of Message
	Tp MessageType
	// Command is the command in this message
	Command *Command
	// PolymorphicEvent represents the row change event
	PolymorphicEvent *model.PolymorphicEvent
	// BarrierTs
	BarrierTs model.Ts
}

// PolymorphicEventMessage creates the message of PolymorphicEvent
func PolymorphicEventMessage(event *model.PolymorphicEvent) *Message {
	return &Message{
		Tp:               MessageTypePolymorphicEvent,
		PolymorphicEvent: event,
	}
}

// CommandMessage creates the message of Command
func CommandMessage(command *Command) *Message {
	return &Message{
		Tp:      MessageTypeCommand,
		Command: command,
	}
}

// BarrierMessage creates the message of Command
func BarrierMessage(barrierTs model.Ts) *Message {
	return &Message{
		Tp:        MessageTypeBarrier,
		BarrierTs: barrierTs,
	}
}

// TickMessage is called frequently,
// to ease GC pressure we return a global variable.
var tickMsg *Message = &Message{
	Tp: MessageTypeTick,
}

// TickMessage creates the message of Tick
func TickMessage() *Message {
	return tickMsg
}

// CommandType is the type of Command
type CommandType int

const (
	// CommandTypeUnknown is unknown message type
	CommandTypeUnknown CommandType = iota
	// CommandTypeStopAtTs means the table pipeline should stop at the specified Ts
	CommandTypeStopAtTs
)

// Command is the command about table pipeline
type Command struct {
	Tp        CommandType
	StoppedTs model.Ts
}
