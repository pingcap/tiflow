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
	"fmt"
	"net/url"

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/model"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
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

// BuildResPath builds a resource path from the given scope.
func (s ResourceScope) BuildResPath() string {
	if s.Executor == "" {
		return ""
	}
	resPath := url.QueryEscape(string(s.Executor))
	if s.WorkerID != "" {
		resPath += fmt.Sprintf("/%s", url.QueryEscape(s.WorkerID))
	}
	return resPath
}

// ResourceIdent provides information for the file manager to
// uniquely determine where and how the resource is stored.
type ResourceIdent struct {
	ResourceScope

	// Name is the custom part of the resourceID.
	// For example, the resource name of `/local/resource-1` is `resource-1`.
	Name resModel.ResourceName
}

// Scope returns the Scope of the ResourceIdent.
func (i ResourceIdent) Scope() ResourceScope {
	return i.ResourceScope
}

// BuildResPath builds a resource path from the given ident.
func (i ResourceIdent) BuildResPath() string {
	return fmt.Sprintf("%s/%s", i.Scope().BuildResPath(), url.QueryEscape(i.Name))
}

// FileManager abstracts the operations on the underlying storage.
type FileManager interface {
	// CreateResource creates a new resource.
	CreateResource(ctx context.Context, ident ResourceIdent) (ResourceDescriptor, error)

	// GetPersistedResource returns the descriptor of an already persisted resource.
	GetPersistedResource(ctx context.Context, ident ResourceIdent) (ResourceDescriptor, error)

	// CleanOrRecreatePersistedResource cleans or recreates the persisted resource.
	// For local filemanager, it simply removes the resource and recreates it.
	// For s3 filemanager, it either cleans files or recreates resources.
	CleanOrRecreatePersistedResource(ctx context.Context, ident ResourceIdent) (ResourceDescriptor, error)

	// RemoveTemporaryFiles cleans up all un-persisted resource files under the scope.
	RemoveTemporaryFiles(ctx context.Context, scope ResourceScope) error

	// RemoveResource removes a resource's files.
	RemoveResource(ctx context.Context, ident ResourceIdent) error

	// SetPersisted sets a resource as persisted.
	SetPersisted(ctx context.Context, ident ResourceIdent) error
}
