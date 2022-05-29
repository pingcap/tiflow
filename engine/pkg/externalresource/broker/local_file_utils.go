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
	"context"
	"encoding/hex"
	"path/filepath"
	"testing"

	"github.com/pingcap/errors"
	brStorage "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"

	libModel "github.com/pingcap/tiflow/engine/lib/model"
	derrors "github.com/pingcap/tiflow/engine/pkg/errors"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
)

func newBrStorageForLocalFile(filePath string) (brStorage.ExternalStorage, error) {
	backend, err := brStorage.ParseBackend(filePath, nil)
	if err != nil {
		return nil, err
	}
	ls, err := brStorage.New(context.Background(), backend, nil)
	if err != nil {
		return nil, derrors.ErrFailToCreateExternalStorage.Wrap(err)
	}
	return ls, nil
}

func resourceNameToFilePathName(resName model.ResourceName) string {
	return hex.EncodeToString([]byte(resName))
}

func filePathNameToResourceName(filePath string) (model.ResourceName, error) {
	result, err := hex.DecodeString(filePath)
	if err != nil {
		return "", errors.Trace(err)
	}
	return model.ResourceName(result), nil
}

// AssertLocalFileExists is a test helper.
func AssertLocalFileExists(
	t *testing.T,
	baseDir string,
	creator libModel.WorkerID,
	resName model.ResourceName,
	suffixes ...string,
) {
	joinSegments := []string{
		baseDir, creator, resourceNameToFilePathName(resName),
	}
	joinSegments = append(joinSegments, suffixes...)
	path := filepath.Join(joinSegments...)
	require.FileExistsf(t, path, "local file does not exist: baseDir %s, creator %s, resName %s",
		baseDir, creator, resName)
}

// AssertNoLocalFileExists is a test helper.
func AssertNoLocalFileExists(
	t *testing.T,
	baseDir string,
	creator libModel.WorkerID,
	resName model.ResourceName,
	suffixes ...string,
) {
	joinSegments := []string{
		baseDir, creator, resourceNameToFilePathName(resName),
	}
	joinSegments = append(joinSegments, suffixes...)
	path := filepath.Join(joinSegments...)
	require.NoFileExists(t, path, "local file does not exist: baseDir %s, creator %s, resName %s",
		baseDir, creator, resName)
}
