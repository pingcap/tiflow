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

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/model"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
)

// ResourceScope represents the environment in which the resource
// has been or is to be created.
type ResourceScope struct {
	tenant.ProjectInfo

	// Executor denotes the executor on which the resource is created.
	Executor model.ExecutorID

	// WorkerID denotes the worker that is creating or has created the resource.
	// Note that it is NOT necessarily that consumes or depends on the resource.
	WorkerID frameModel.WorkerID
}

// ResourceIdent provides information for the file manager to
// uniquely determine where and how the resource is stored.
type ResourceIdent struct {
	ResourceScope

	// Name is the custom part of the resourceID.
	// For example, the resource name of `/local/resource-1` is `resource-1`.
	Name resModel.ResourceName
}

func (i ResourceIdent) Scope() ResourceScope {
	return i.ResourceScope
}

// FileManager abstracts the operations on the underlying storage.
type FileManager interface {
	CreateResource(ctx context.Context, ident ResourceIdent) (ResourceDescriptor, error)

	GetPersistedResource(ctx context.Context, ident ResourceIdent) (ResourceDescriptor, error)

	RemoveTemporaryFiles(ctx context.Context, scope ResourceScope) error

	RemoveResource(ctx context.Context, ident ResourceIdent) error

	SetPersisted(ctx context.Context, ident ResourceIdent) error
}
