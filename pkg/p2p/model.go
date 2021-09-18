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

package p2p

type (
	// SenderID represents the identifier of a sender node.
	// Using IP address is not enough because of possible restarts.
	SenderID = string
	// Topic represents the topic for a peer-to-peer message
	Topic = string
)

type (
	Seq = int64
)
