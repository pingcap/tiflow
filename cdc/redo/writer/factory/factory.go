// Copyright 2023 PingCAP, Inc.
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

package factory

import (
	"context"
	"strings"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/redo/writer"
	"github.com/pingcap/tiflow/cdc/redo/writer/blackhole"
	"github.com/pingcap/tiflow/cdc/redo/writer/file"
	"github.com/pingcap/tiflow/cdc/redo/writer/memory"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/redo"
)

// NewRedoLogWriter creates a new RedoLogWriter.
func NewRedoLogWriter(
	ctx context.Context, lwCfg *writer.LogWriterConfig,
) (writer.RedoLogWriter, error) {
	uri, err := storage.ParseRawURL(lwCfg.Storage)
	if err != nil {
		return nil, err
	}

	if !redo.IsValidConsistentStorage(uri.Scheme) {
		return nil, errors.ErrConsistentStorage.GenWithStackByArgs(uri.Scheme)
	}

	lwCfg.URI = uri
	lwCfg.UseExternalStorage = redo.IsExternalStorage(uri.Scheme)

	if redo.IsBlackholeStorage(uri.Scheme) {
		invalid := strings.HasSuffix(uri.Scheme, "invalid")
		return blackhole.NewLogWriter(invalid), nil
	}

	if lwCfg.UseFileBackend {
		return file.NewLogWriter(ctx, lwCfg)
	}
	return memory.NewLogWriter(ctx, lwCfg)
}
