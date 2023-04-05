//  Copyright 2021 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

package reader

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/stretchr/testify/require"
)

func TestReaderNewReader(t *testing.T) {
	_, err := newReaders(context.Background(), nil)
	require.NotNil(t, err)

	dir := t.TempDir()
	require.Panics(t, func() {
		_, err = newReaders(context.Background(), &readerConfig{dir: dir})
	})
}

func TestFileReaderRead(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uri, err := url.Parse(fmt.Sprintf("file://%s", dir))
	require.NoError(t, err)
	cfg := &readerConfig{
		dir:                t.TempDir(),
		startTs:            10,
		endTs:              12,
		fileType:           redo.RedoRowLogFileType,
		uri:                *uri,
		useExternalStorage: true,
	}
	// log file with maxCommitTs<=startTs, fileter when download file
	genLogFile(ctx, t, dir, redo.RedoRowLogFileType, 1, cfg.startTs)
	// normal log file, include [10, 11, 12] and [11, 12, ... 20]
	genLogFile(ctx, t, dir, redo.RedoRowLogFileType, cfg.startTs, cfg.endTs+2)
	genLogFile(ctx, t, dir, redo.RedoRowLogFileType, cfg.endTs-1, 20)
	// log file with minCommitTs>endTs, filtered when sort file
	genLogFile(ctx, t, dir, redo.RedoRowLogFileType, 2000, 2023)

	log.Info("start to read redo log files")
	readers, err := newReaders(ctx, cfg)
	require.NoError(t, err)
	require.Equal(t, 2, len(readers))
	defer readers[0].Close() //nolint:errcheck

	for _, r := range readers {
		log, err := r.Read()
		require.NoError(t, err)
		require.EqualValues(t, 11, log.RedoRow.Row.CommitTs)
		log, err = r.Read()
		require.NoError(t, err)
		require.EqualValues(t, 12, log.RedoRow.Row.CommitTs)
		log, err = r.Read()
		require.Nil(t, log)
		require.ErrorIs(t, err, io.EOF)
		require.NoError(t, r.Close())
	}
}
