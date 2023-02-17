//  Copyright 2022 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

//go:generate msgp

package common

import (
	"github.com/pingcap/tiflow/cdc/model"
)

// LogMeta is used for store meta info.
type LogMeta struct {
	CheckpointTs uint64 `msg:"checkpointTs"`
	ResolvedTs   uint64 `msg:"resolvedTs"`
}

// ParseMeta parses meta.
func ParseMeta(metas []*LogMeta, checkpointTs, resolvedTs *model.Ts) {
	*checkpointTs = 0
	*resolvedTs = 0
	for _, meta := range metas {
		if *checkpointTs < meta.CheckpointTs {
			*checkpointTs = meta.CheckpointTs
		}
		if *resolvedTs < meta.ResolvedTs {
			*resolvedTs = meta.ResolvedTs
		}
	}
}
