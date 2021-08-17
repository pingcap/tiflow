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

package sorter

import (
	"encoding/hex"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

type msgPackGenSerde struct {
}

func (m *msgPackGenSerde) marshal(event *model.PolymorphicEvent, bytes []byte) ([]byte, error) {
	bytes = bytes[:0]
	log.Debug("2400: msgPackGenSerde.marshal event.RawKV", zap.Uint64("StartTs", event.RawKV.StartTs), zap.String("Key", hex.EncodeToString(event.RawKV.Key)))
	return event.RawKV.MarshalMsg(bytes)
}

func (m *msgPackGenSerde) unmarshal(event *model.PolymorphicEvent, bytes []byte) ([]byte, error) {
	if event.RawKV == nil {
		event.RawKV = new(model.RawKVEntry)
	}

	bytes, err := event.RawKV.UnmarshalMsg(bytes)
	if err != nil {
		return nil, errors.Trace(err)
	}

	event.StartTs = event.RawKV.StartTs
	event.CRTs = event.RawKV.CRTs
	log.Debug("2400: msgPackGenSerde.unmarshal event.RawKV", zap.Uint64("StartTs", event.RawKV.StartTs), zap.String("Key", hex.EncodeToString(event.RawKV.Key)))

	return bytes, nil
}
