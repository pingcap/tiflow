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

package internal

import (
	"context"

	brStorage "github.com/pingcap/tidb/br/pkg/storage"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
)

// ResourceDescriptor is an object used internally by the broker
// to manage resources.
type ResourceDescriptor interface {
	URI() string
	ID() resModel.ResourceID
	ResourceIdent() ResourceIdent
	ExternalStorage(ctx context.Context) (brStorage.ExternalStorage, error)
}
