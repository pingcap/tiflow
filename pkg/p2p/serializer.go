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

import "encoding/json"

// Serializable is an interface for defining custom serialization methods
// for peer messages.
type Serializable interface {
	Marshal() ([]byte, error)
	Unmarshal(data []byte) error
}

func marshalMessage(value interface{}) ([]byte, error) {
	if value, ok := value.(Serializable); ok {
		return value.Marshal()
	}
	return json.Marshal(value)
}

func unmarshalMessage(data []byte, value interface{}) error {
	if value, ok := value.(Serializable); ok {
		return value.Unmarshal(data)
	}
	return json.Unmarshal(data, value)
}
