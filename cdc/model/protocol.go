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

package model

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/p2p"
	"github.com/vmihailenco/msgpack/v5"
)

// This file contains a communication protocol between the Owner and the Processor.
// FIXME a detailed documentation on the interaction will be added later in a separate file.

// DispatchTableTopic returns a message topic for dispatching a table.
func DispatchTableTopic(changefeedID ChangeFeedID) p2p.Topic {
	return fmt.Sprintf("dispatch/%s", changefeedID)
}

// DispatchTableMessage is the message body for dispatching a table.
type DispatchTableMessage struct {
	OwnerRev   int64   `json:"owner-rev"`
	ID         TableID `json:"id"`
	IsDelete   bool    `json:"is-delete"`
}

// DispatchTableResponseTopic returns a message topic for the result of
// dispatching a table. It is sent from the Processor to the Owner.
func DispatchTableResponseTopic(changefeedID ChangeFeedID) p2p.Topic {
	return fmt.Sprintf("dispatch-resp/%s", changefeedID)
}

// DispatchTableResponseMessage is the message body for the result of dispatching a table.
type DispatchTableResponseMessage struct {
	ID TableID `json:"id"`
}

// AnnounceTopic returns a message topic for announcing an ownership change.
func AnnounceTopic(changefeedID ChangeFeedID) p2p.Topic {
	return fmt.Sprintf("send-status/%s", changefeedID)
}

// AnnounceMessage is the message body for announcing an ownership change.
type AnnounceMessage struct {
	OwnerRev int64 `json:"owner-rev"`
}

// SyncTopic returns a message body for syncing the current states of a processor.
func SyncTopic(changefeedID ChangeFeedID) p2p.Topic {
	return fmt.Sprintf("send-status-resp/%s", changefeedID)
}

// SyncMessage is the message body for syncing the current states of a processor.
// MsgPack serialization has been implemented to minimize the size of the message.
type SyncMessage struct {
	Running  []TableID
	Adding   []TableID
	Removing []TableID
}

// Marshal serializes the message into MsgPack format.
func (m *SyncMessage) Marshal() ([]byte, error) {
	raw, err := msgpack.Marshal(m)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return raw, nil
}

// Unmarshal deserializes the message.
func (m *SyncMessage) Unmarshal(data []byte) error {
	return msgpack.Unmarshal(data, m)
}

// CheckpointTopic returns a topic for sending the latest checkpoint from
// the Processor to the Owner.
func CheckpointTopic(changefeedID ChangeFeedID) p2p.Topic {
	return fmt.Sprintf("checkpoint/%s", changefeedID)
}

// CheckpointMessage is the message body for sending the latest checkpoint
// from the Processor to the Owner.
type CheckpointMessage struct {
	CheckpointTs Ts `json:"checkpoint-ts"`
	ResolvedTs   Ts `json:"resolved-ts"`
}
