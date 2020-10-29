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
	// MessageTypeLifecycle is lifecycle message type
	MessageTypeLifecycle
	// MessageTypeRowChangedEvent is the row changed event message type
	MessageTypeRowChangedEvent
)

// Message is a vehicle for transferring information between nodes
type Message struct {
	// TODO add more kind of messages
	// Tp is the type of Message
	Tp MessageType
	// Lifecycle represents the message about the lifecycle
	Lifecycle *LifecycleMessage
	// RowChangedEvent represents the row change event
	RowChangedEvent *model.RowChangedEvent
}

// SetLifecycleMessage sets the message to LifecycleMessage
func (m *Message) SetLifecycleMessage(msg *LifecycleMessage) *Message {
	m.Tp = MessageTypeLifecycle
	m.Lifecycle = msg
	return m
}

// SetRowChangedEvent sets the message to RowChangedEvent
func (m *Message) SetRowChangedEvent(row *model.RowChangedEvent) *Message {
	m.Tp = MessageTypeRowChangedEvent
	m.RowChangedEvent = row
	return m
}

// LifecycleMessageType is the type of message about the lifecycle
type LifecycleMessageType int

const (
	// LifecycleMessageUnknown is unknown lifecycle message
	LifecycleMessageUnknown LifecycleMessageType = iota
	// LifecycleMessageStarted is started lifecycle message
	LifecycleMessageStarted
	// LifecycleMessageStopped is stopped lifecycle message
	LifecycleMessageStopped
)

// LifecycleMessage represents the message about the lifecycle
type LifecycleMessage struct {
	Tp LifecycleMessageType
}

// SetStarted sets the message to Started
func (m *LifecycleMessage) SetStarted() *LifecycleMessage {
	m.Tp = LifecycleMessageStarted
	return m
}

// SetStopped sets the message to Stopped
func (m *LifecycleMessage) SetStopped() *LifecycleMessage {
	m.Tp = LifecycleMessageStopped
	return m
}
