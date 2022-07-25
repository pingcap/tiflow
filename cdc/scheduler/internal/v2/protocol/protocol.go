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

package protocol

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/vmihailenco/msgpack/v5"
)

// This file contains a communication protocol between the Owner and the Processor.
// FIXME a detailed documentation on the interaction will be added later in a separate file.

// DispatchTableTopic returns a message topic for dispatching a table.
func DispatchTableTopic(changefeedID model.ChangeFeedID) p2p.Topic {
	return fmt.Sprintf("dispatch/%s/%s", changefeedID.Namespace, changefeedID.ID)
}

// DispatchTableMessage is the message body for dispatching a table.
type DispatchTableMessage struct {
	OwnerRev int64          `json:"owner-rev"`
	Epoch    ProcessorEpoch `json:"epoch"`
	ID       model.TableID  `json:"id"`
	StartTs  model.Ts       `json:"start-ts"`
	IsDelete bool           `json:"is-delete"`
}

// DispatchTableResponseTopic returns a message topic for the result of
// dispatching a table. It is sent from the Processor to the Owner.
func DispatchTableResponseTopic(changefeedID model.ChangeFeedID) p2p.Topic {
	return fmt.Sprintf("dispatch-resp/%s/%s", changefeedID.Namespace, changefeedID.ID)
}

// DispatchTableResponseMessage is the message body for the result of dispatching a table.
type DispatchTableResponseMessage struct {
	ID    model.TableID  `json:"id"`
	Epoch ProcessorEpoch `json:"epoch"`
}

// AnnounceTopic returns a message topic for announcing an ownership change.
func AnnounceTopic(changefeedID model.ChangeFeedID) p2p.Topic {
	return fmt.Sprintf("send-status/%s/%s", changefeedID.Namespace, changefeedID.ID)
}

// AnnounceMessage is the message body for announcing an ownership change.
type AnnounceMessage struct {
	OwnerRev int64 `json:"owner-rev"`
	// Sends the owner's version for compatibility check
	OwnerVersion string `json:"owner-version"`
}

// SyncTopic returns a message body for syncing the current states of a processor.
func SyncTopic(changefeedID model.ChangeFeedID) p2p.Topic {
	return fmt.Sprintf("send-status-resp/%s/%s", changefeedID.Namespace, changefeedID.ID)
}

// ProcessorEpoch designates a continuous period of the processor working normally.
type ProcessorEpoch = string

// SyncMessage is the message body for syncing the current states of a processor.
// MsgPack serialization has been implemented to minimize the size of the message.
type SyncMessage struct {
	// Sends the processor's version for compatibility check
	ProcessorVersion string

	// Epoch is reset to a unique value when the processor has
	// encountered an internal error or other events so that
	// it has to re-sync its states with the Owner.
	Epoch ProcessorEpoch

	Running  []model.TableID
	Adding   []model.TableID
	Removing []model.TableID
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
func CheckpointTopic(changefeedID model.ChangeFeedID) p2p.Topic {
	return fmt.Sprintf("checkpoint/%s/%s", changefeedID.Namespace, changefeedID.ID)
}

// CheckpointMessage is the message body for sending the latest checkpoint
// from the Processor to the Owner.
type CheckpointMessage struct {
	CheckpointTs model.Ts `json:"checkpoint-ts"`
	ResolvedTs   model.Ts `json:"resolved-ts"`
}
