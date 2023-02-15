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

package dm

import (
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
)

// GetDMStorageType return the storage type that DM uses
func GetDMStorageType(bucketEnabled bool, bucketType resModel.ResourceType) resModel.ResourceType {
	resType := resModel.ResourceTypeLocalFile
	if bucketEnabled {
		resType = bucketType
	}

	return resType
}

// NewDMResourceID returns a ResourceID in DM's style. Currently only support s3/gcs resource.
func NewDMResourceID(taskName, sourceName string, resType resModel.ResourceType) resModel.ResourceID {
	return "/" + string(resType) + "/" + taskName + "-" + sourceName
}
