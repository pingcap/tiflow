// Copyright 2022 PingCAP, Inc.
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
	"context"

	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
)

// KVExt extends the KV interface with Do method to implement the intermediate
type KVExt interface {
	metaModel.KV

	// Do applies a single Op on KV without a transaction.
	// Do is useful when adding intermidate layer to KV implement
	Do(ctx context.Context, op metaModel.Op) (metaModel.OpResponse, metaModel.Error)
}
