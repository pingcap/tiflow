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
	"path/filepath"
	"strings"

	libModel "github.com/pingcap/tiflow/engine/lib/model"
)

// LocalFileResourceDescriptor contains necessary data
// to access a local file resource.
type LocalFileResourceDescriptor struct {
	BasePath     string
	Creator      libModel.WorkerID
	ResourceName ResourceName
}

// AbsolutePath returns the absolute path of the given resource
// in the local file system.
func (d *LocalFileResourceDescriptor) AbsolutePath() string {
	escapedName := strings.ReplaceAll(d.ResourceName, "/", "_")
	return filepath.Join(d.BasePath, d.Creator, escapedName)
}
