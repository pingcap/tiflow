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

package schedulepb

import "github.com/pingcap/tiflow/cdc/model"

// Watermark contains various ts variables to make code easier
type Watermark struct {
	CheckpointTs     model.Ts
	ResolvedTs       model.Ts
	LastSyncedTs     model.Ts
	PullerResolvedTs model.Ts
}
