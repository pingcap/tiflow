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

// Type is the type of Message
type Type int

// types of Message
const (
	TypeUnknown Type = iota
	TypeStop
	TypeValue
)

// Message is a vehicle for transferring information between nodes
type Message[T any] struct {
	// Tp is the type of Message
	Tp Type

	Value T
}

// StopMessage creates the message of Stop.
// After receiving a Stop message, actor will be closed.
func StopMessage[T any]() Message[T] {
	return Message[T]{
		Tp: TypeStop,
	}
}

// ValueMessage creates the message of Value.
func ValueMessage[T any](val T) Message[T] {
	return Message[T]{
		Tp:    TypeValue,
		Value: val,
	}
}
