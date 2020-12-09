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

const (
	// MessageTypeUnknown is unknown message type
	MessageTypeUnknown MessageType = iota
	// MessageTypeCommand is command message type
	MessageTypeCommand
	// MessageTypePolymorphicEvent is the row changed event message type
	MessageTypePolymorphicEvent
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

// CommandType is the type of Command
type CommandType int

const (
	// CommandTypeUnknown is unknown message type
	CommandTypeUnknown CommandType = iota
	// CommandTypeShouldStop means the table pipeline should stop soon
	CommandTypeShouldStop
	// CommandTypeStopped means the table pipeline is stopped
	CommandTypeStopped
)

// Command is the command about table pipeline
type Command struct {
	Tp CommandType
}
