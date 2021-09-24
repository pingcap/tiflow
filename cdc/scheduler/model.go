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

package scheduler

import (
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/p2p"
	"github.com/vmihailenco/msgpack/v5"
)

func DispatchTableTopic(changefeedID model.ChangeFeedID) p2p.Topic {
	return fmt.Sprintf("dispatch/%s", changefeedID)
}

type DispatchTableMessage struct {
	OwnerRev   int64         `json:"owner-rev"`
	ID         model.TableID `json:"id"`
	IsDelete   bool          `json:"is-delete"`
	BoundaryTs model.Ts      `json:"boundary-ts"`
}

func DispatchTableResponseTopic(changefeedID model.ChangeFeedID) p2p.Topic {
	return fmt.Sprintf("dispatch-resp/%s", changefeedID)
}

type DispatchTableResponseMessage struct {
	ID model.TableID `json:"id"`
}

func AnnounceTopic(changefeedID model.ChangeFeedID) p2p.Topic {
	return fmt.Sprintf("send-status/%s", changefeedID)
}

type AnnounceMessage struct {
	OwnerRev int64 `json:"owner-rev"`
}

func SyncTopic(changefeedID model.ChangeFeedID) p2p.Topic {
	return fmt.Sprintf("send-status-resp/%s", changefeedID)
}

type SyncMessage struct {
	Running  []model.TableID
	Adding   []model.TableID
	Removing []model.TableID
}

func (m *SyncMessage) Marshal() ([]byte, error) {
	raw, err := msgpack.Marshal(m)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return json.Marshal(raw)
}

func (m *SyncMessage) Unmarshal(data []byte) error {
	var raw []byte
	err := json.Unmarshal(data, &raw)
	if err != nil {
		return errors.Trace(err)
	}
	return msgpack.Unmarshal(raw, m)
}

func CheckpointTopic(changefeedID model.ChangeFeedID) p2p.Topic {
	return fmt.Sprintf("checkpoint/%s", changefeedID)
}

type CheckpointMessage struct {
	CheckpointTs model.Ts `json:"checkpoint-ts"`
	ResolvedTs   model.Ts `json:"resolved-ts"`
}
