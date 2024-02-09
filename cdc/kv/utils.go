// Copyright 2023 PingCAP, Inc.
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

package kv

import (
	"github.com/pingcap/kvproto/pkg/cdcpb"
)

func getChangeDataEventCommitTs(cevent *cdcpb.ChangeDataEvent) (commitTs uint64) {
	if len(cevent.Events) != 0 {
		if entries, ok := cevent.Events[0].Event.(*cdcpb.Event_Entries_); ok && len(entries.Entries.Entries) > 0 {
			commitTs = entries.Entries.Entries[0].CommitTs
		}
	}
	return
}
