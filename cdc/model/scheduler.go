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
	"encoding/json"
	"fmt"

	"github.com/pingcap/errors"

	"github.com/pingcap/ticdc/pkg/p2p"
	"github.com/vmihailenco/msgpack/v5"
)

func DispatchTableTopic(changefeedID ChangeFeedID) p2p.Topic {
	return p2p.Topic(fmt.Sprintf("dispatch/%s", changefeedID))
}

type DispatchTableMessage struct {
	OwnerRev   int64   `json:"owner-rev"`
	ID         TableID `json:"id"`
	IsDelete   bool    `json:"is-delete"`
	BoundaryTs Ts      `json:"boundary-ts"`

	// For internal use by the processor
	Processed bool `json:"-"`
}

func DispatchTableResponseTopic(changefeedID ChangeFeedID) p2p.Topic {
	return p2p.Topic(fmt.Sprintf("dispatch-resp/%s", changefeedID))
}

type DispatchTableResponseMessage struct {
	ID TableID `json:"id"`
}

func RequestSendTableStatusTopic(changefeedID ChangeFeedID) p2p.Topic {
	return p2p.Topic(fmt.Sprintf("send-status/%s", changefeedID))
}

type RequestSendTableStatusMessage struct {
	OwnerRev int64 `json:"owner-rev"`
}

func RequestSendTableStatusResponseTopic(changefeedID ChangeFeedID) p2p.Topic {
	return p2p.Topic(fmt.Sprintf("send-status-resp/%s", changefeedID))
}

type RequestSendTableStatusResponseMessage struct {
	Running  []TableID
	Adding   []TableID
	Removing []TableID
}

func (m *RequestSendTableStatusResponseMessage) MarshalJSON() ([]byte, error) {
	raw, err := msgpack.Marshal(m)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return json.Marshal(raw)
}

func (m *RequestSendTableStatusResponseMessage) UnmarshalJSON(data []byte) error {
	var raw []byte
	err := json.Unmarshal(data, &raw)
	if err != nil {
		return errors.Trace(err)
	}
	return msgpack.Unmarshal(raw, m)
}

func ProcessorFailedTopic(changefeedID ChangeFeedID) p2p.Topic {
	return p2p.Topic(fmt.Sprintf("processor-fail/%s", changefeedID))
}

type ProcessorFailedMessage struct {
	ProcessorID CaptureID `json:"capture-id"`
}
