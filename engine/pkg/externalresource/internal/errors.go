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
	"github.com/pingcap/tiflow/engine/model"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	"github.com/pingcap/tiflow/engine/pkg/rpcerror"
)

// ResourceNotFoundErrorInfo provides details of ErrResourceNotFound.
type ResourceNotFoundErrorInfo struct {
	rpcerror.Error[rpcerror.NotRetryable, rpcerror.NotFound]

	ResourceID resModel.ResourceID
	Details    string
}

// ErrResourceNotFound indicates that a resource is not found.
var ErrResourceNotFound = rpcerror.Normalize[ResourceNotFoundErrorInfo]()

// ResourceMetastoreError provides details of ErrResourceMetastoreError.
type ResourceMetastoreError struct {
	rpcerror.Error[rpcerror.Retryable, rpcerror.Aborted]

	ResourceID resModel.ResourceID
	Details    string
}

// ErrResourceMetastoreError indicates that there is a failure in querying the
// metastore on a resource, but the error can be non-fatal.
var ErrResourceMetastoreError = rpcerror.Normalize[ResourceMetastoreError]()

// ResourceAlreadyExistsErrorInfo provides details of ErrResourceAlreadyExists
type ResourceAlreadyExistsErrorInfo struct {
	rpcerror.Error[rpcerror.Retryable, rpcerror.AlreadyExists]

	ResourceID resModel.ResourceID
	Details    string
}

// ErrResourceAlreadyExists indicates that a resource already exists.
var ErrResourceAlreadyExists = rpcerror.Normalize[ResourceAlreadyExistsErrorInfo]()

// InvalidArgumentErrorInfo provides details of ErrInvalidArgument
type InvalidArgumentErrorInfo struct {
	rpcerror.Error[rpcerror.NotRetryable, rpcerror.InvalidArgument]

	ResourceID resModel.ResourceID
	JobID      model.JobID
	Annotation string // description in English why the argument is invalid
}

// ErrInvalidArgument indicates that a resource-related request has an invalid argument.
var ErrInvalidArgument = rpcerror.Normalize[InvalidArgumentErrorInfo]()
