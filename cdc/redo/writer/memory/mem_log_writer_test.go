//  Copyright 2023 PingCAP, Inc.
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

package memory

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/writer"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestWriteDDL(t *testing.T) {
	t.Parallel()

	rows := []writer.RedoEvent{
		nil,
		&model.RowChangedEvent{
			PhysicalTableID: 11,
			CommitTs:        11,
			TableInfo:       &model.TableInfo{TableName: model.TableName{Schema: "test", Table: "t1"}},
		},
		&model.RowChangedEvent{
			PhysicalTableID: 12,
			CommitTs:        15,
			TableInfo:       &model.TableInfo{TableName: model.TableName{Schema: "test", Table: "t2"}},
		},
		&model.RowChangedEvent{
			PhysicalTableID: 12,
			CommitTs:        8,
			TableInfo:       &model.TableInfo{TableName: model.TableName{Schema: "test", Table: "t2"}},
		},
	}
	testWriteEvents(t, rows)
}

func TestWriteDML(t *testing.T) {
	t.Parallel()

	ddls := []writer.RedoEvent{
		nil,
		&model.DDLEvent{CommitTs: 1},
		&model.DDLEvent{CommitTs: 10},
		&model.DDLEvent{CommitTs: 8},
	}
	testWriteEvents(t, ddls)
}

func testWriteEvents(t *testing.T, events []writer.RedoEvent) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	extStorage, uri, err := util.GetTestExtStorage(ctx, t.TempDir())
	require.NoError(t, err)
	lwcfg := &writer.LogWriterConfig{
		LogType:            redo.RedoDDLLogFileType,
		CaptureID:          "test-capture",
		ChangeFeedID:       model.DefaultChangeFeedID("test-changefeed"),
		URI:                uri,
		UseExternalStorage: true,
		MaxLogSizeInBytes:  10 * redo.Megabyte,
	}
	filename := t.Name()
	lw, err := NewLogWriter(ctx, lwcfg, writer.WithLogFileName(func() string {
		return filename
	}))
	require.NoError(t, err)

	require.NoError(t, lw.WriteEvents(ctx, events...))
	require.Eventually(t, func() bool {
		if len(lw.encodeWorkers.outputCh) != 0 {
			log.Warn(fmt.Sprintf("eventCh len %d", len(lw.encodeWorkers.outputCh)))
		}
		return len(lw.encodeWorkers.outputCh) == 0
	}, 2*time.Second, 10*time.Millisecond)

	// test flush
	require.NoError(t, lw.FlushLog(ctx))
	err = extStorage.WalkDir(ctx, nil, func(path string, size int64) error {
		require.Equal(t, filename, path)
		return nil
	})
	require.NoError(t, err)

	require.ErrorIs(t, lw.Close(), context.Canceled)
	require.Eventually(t, func() bool {
		err = lw.WriteEvents(ctx, events...)
		return err != nil
	}, 2*time.Second, 10*time.Millisecond)
	require.ErrorIs(t, err, cerror.ErrRedoWriterStopped)
	err = lw.FlushLog(ctx)
	require.ErrorIs(t, err, cerror.ErrRedoWriterStopped)
}
