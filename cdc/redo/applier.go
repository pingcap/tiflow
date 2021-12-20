// Copyright 2021 PingCAP, Inc.
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

package redo

import (
	"context"

	"github.com/pingcap/tiflow/cdc/redo/reader"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// NewRedoReader creates a new redo log reader
func NewRedoReader(ctx context.Context, storage string, cfg *reader.LogReaderConfig) (rd reader.RedoLogReader, err error) {
	switch consistentStorage(storage) {
	case consistentStorageBlackhole:
		rd = reader.NewBlackHoleReader()
	case consistentStorageLocal, consistentStorageNFS, consistentStorageS3:
		rd, err = reader.NewLogReader(ctx, cfg)
	default:
		err = cerror.ErrConsistentStorage.GenWithStackByArgs(storage)
	}
	return
}
