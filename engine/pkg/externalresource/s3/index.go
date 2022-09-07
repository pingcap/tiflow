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

package s3

import (
	"context"

	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
)

type indexManager interface {
	SetPersisted(ctx context.Context, ident internal.ResourceIdent) (bool, error)
	UnsetPersisted(ctx context.Context, ident internal.ResourceIdent) (bool, error)
}

type indexManagerImpl struct {
}
