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
	"fmt"
	"path"

	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
)

const (
	// DummyWorkerID is a dummy worker ID used for the s3 storage.
	DummyWorkerID = "keep-alive-worker"
	// DummyResourceID is a dummy resource ID used for the s3 storage.
	DummyResourceID = "/s3/dummy"
)

var (
	dummyJobID        = "dummy-job-%s"
	dummyResourceName = resModel.EncodeResourceName("dummy")
)

// GetDummyIdent returns a dummy resource ident for testing.
func GetDummyIdent(executorID model.ExecutorID) internal.ResourceIdent {
	return internal.ResourceIdent{
		Name: GetDummyResourceName(),
		ResourceScope: internal.ResourceScope{
			Executor: executorID,
			WorkerID: DummyWorkerID,
		},
	}
}

// GetDummyResourceName returns a dummy resource name for s3 storage.
func GetDummyResourceName() string {
	return dummyResourceName
}

// GetDummyResourceKey returns a dummy resource key for s3 storage.
func GetDummyResourceKey(executorID model.ExecutorID) resModel.ResourceKey {
	return resModel.ResourceKey{
		JobID: GetDummyJobID(executorID),
		ID:    DummyResourceID,
	}
}

// GetDummyJobID returns a dummy job ID for s3 storage.
func GetDummyJobID(executorID model.ExecutorID) model.JobID {
	return fmt.Sprintf(dummyJobID, executorID)
}

// GetDummyResPath returns a file path located in dummy resource for s3 storage.
func GetDummyResPath(filename string) string {
	return path.Join(DummyWorkerID, dummyResourceName, filename)
}
