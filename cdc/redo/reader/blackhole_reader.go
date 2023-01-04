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

package reader

import (
	"context"

	"github.com/pingcap/tiflow/cdc/model"
)

// BlackHoleReader is a blockHole storage which implements LogReader interface
type BlackHoleReader struct{}

// newBlackHoleReader creates a new BlackHoleReader
func newBlackHoleReader() *BlackHoleReader {
	return &BlackHoleReader{}
}

// ResetReader implements LogReader.ReadLog
func (br *BlackHoleReader) ResetReader(ctx context.Context, startTs, endTs uint64) error {
	return nil
}

// ReadNextLog implements LogReader.ReadNextLog
func (br *BlackHoleReader) ReadNextLog(ctx context.Context, maxNumberOfEvents uint64) ([]*model.RedoRowChangedEvent, error) {
	return nil, nil
}

// ReadNextDDL implements LogReader.ReadNextDDL
func (br *BlackHoleReader) ReadNextDDL(ctx context.Context, maxNumberOfEvents uint64) ([]*model.RedoDDLEvent, error) {
	return nil, nil
}

// ReadMeta implements LogReader.ReadMeta
func (br *BlackHoleReader) ReadMeta(ctx context.Context) (checkpointTs, resolvedTs uint64, err error) {
	return 0, 1, nil
}

// Close implement the Close interface
func (br *BlackHoleReader) Close() error {
	return nil
}
