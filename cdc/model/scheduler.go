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

	"github.com/pingcap/ticdc/pkg/p2p"
)

type DispatchTableMessage struct {
	OwnerRev   int64   `json:"owner-rev"`
	ID         TableID `json:"id"`
	IsDelete   bool    `json:"is-delete"`
	BoundaryTs Ts      `json:"boundary-ts"`

	// For internal use by the processor
	Processed bool `json:"-"`
}

type DispatchTableResponseMessage struct {
	ID TableID `json:"id"`
}

func DispatchTableTopic(changefeedID ChangeFeedID) p2p.Topic {
	return p2p.Topic(fmt.Sprintf("dispatch/%s", changefeedID))
}

func DispatchTableResponseTopic(changefeedID ChangeFeedID) p2p.Topic {
	return p2p.Topic(fmt.Sprintf("dispatch-resp/%s", changefeedID))
}
