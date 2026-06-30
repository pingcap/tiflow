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

package local

import (
	"context"
	"encoding/hex"
	"path/filepath"
	"testing"

	brStorage "github.com/pingcap/tidb/br/pkg/storage"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func newBrStorageForLocalFile(filePath string) (brStorage.ExternalStorage, error) {
	backend, err := brStorage.ParseBackend(filePath, nil)
	if err != nil {
		return nil, err
	}
	ls, err := brStorage.New(context.Background(), backend, nil)
	if err != nil {
		retErr := errors.ErrFailToCreateExternalStorage.Wrap(err)
		return nil, retErr.GenWithStackByArgs("creating ExternalStorage for local file")
	}
	return ls, nil
}

// ResourceNameToFilePathName converts a resource name to a file path name.
func ResourceNameToFilePathName(resName resModel.ResourceName) string {
	return hex.EncodeToString([]byte(resName))
}

func filePathNameToResourceName(filePath string) (resModel.ResourceName, error) {
	result, err := hex.DecodeString(filePath)
	if err != nil {
		return "", errors.Trace(err)
	}
	return resModel.ResourceName(result), nil
}

func localPathWithEncoding(baseDir string,
	creator frameModel.WorkerID,
	resName resModel.ResourceName,
	suffixes ...string,
) string {
	joinSegments := []string{
		baseDir, creator, ResourceNameToFilePathName(resName),
	}
	joinSegments = append(joinSegments, suffixes...)
	return filepath.Join(joinSegments...)
}

// AssertLocalFileExists is a test helper.
func AssertLocalFileExists(
	t *testing.T,
	baseDir string,
	creator frameModel.WorkerID,
	resName resModel.ResourceName,
	suffixes ...string,
) {
	require.FileExistsf(t, localPathWithEncoding(baseDir, creator, resName, suffixes...),
		"local file does not exist: baseDir %s, creator %s, resName %s",
		baseDir, creator, resName)
}

// AssertNoLocalFileExists is a test helper.
func AssertNoLocalFileExists(
	t *testing.T,
	baseDir string,
	creator frameModel.WorkerID,
	resName resModel.ResourceName,
	suffixes ...string,
) {
	require.NoFileExists(t, localPathWithEncoding(baseDir, creator, resName, suffixes...),
		"local file does not exist: baseDir %s, creator %s, resName %s",
		baseDir, creator, resName)
}
