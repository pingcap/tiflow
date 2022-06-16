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

package broker

import (
	"path/filepath"

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
)

// LocalFileResourceDescriptor contains necessary data
// to access a local file resource.
type LocalFileResourceDescriptor struct {
	BasePath     string
	Creator      frameModel.WorkerID
	ResourceName resModel.ResourceName
}

// AbsolutePath returns the absolute path of the given resource
// in the local file system.
func (d *LocalFileResourceDescriptor) AbsolutePath() string {
	encodedName := resourceNameToFilePathName(d.ResourceName)
	return filepath.Join(d.BasePath, d.Creator, encodedName)
}
