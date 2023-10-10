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

package file

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	mockstorage "github.com/pingcap/tidb/br/pkg/mock/storage"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/cdc/redo/writer"
	"github.com/pingcap/tiflow/pkg/fsutil"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/pingcap/tiflow/pkg/uuid"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/atomic"
	"go.uber.org/mock/gomock"
)

func TestWriterWrite(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfs := []model.ChangeFeedID{
		model.DefaultChangeFeedID("test-cf"),
		{
			Namespace: "abcd",
			ID:        "test-cf",
		},
	}

	cf11s := []model.ChangeFeedID{
		model.DefaultChangeFeedID("test-cf11"),
		{
			Namespace: "abcd",
			ID:        "test-cf11",
		},
	}

	for idx, cf := range cfs {
		uuidGen := uuid.NewConstGenerator("const-uuid")
		w := &Writer{
			cfg: &writer.LogWriterConfig{
				MaxLogSizeInBytes: 10,
				Dir:               dir,
				ChangeFeedID:      cf,
				CaptureID:         "cp",
				LogType:           redo.RedoRowLogFileType,
			},
			uint64buf: make([]byte, 8),
			running:   *atomic.NewBool(true),
			metricWriteBytes: common.RedoWriteBytesGauge.
				WithLabelValues("default", "test-cf"),
			metricFsyncDuration: common.RedoFsyncDurationHistogram.
				WithLabelValues("default", "test-cf"),
			metricFlushAllDuration: common.RedoFlushAllDurationHistogram.
				WithLabelValues("default", "test-cf"),
			uuidGenerator: uuidGen,
		}

		w.eventCommitTS.Store(1)
		_, err := w.Write([]byte("tes1t11111"))
		require.Nil(t, err)
		var fileName string
		// create a .tmp file
		if w.cfg.ChangeFeedID.Namespace == model.DefaultNamespace {
			fileName = fmt.Sprintf(redo.RedoLogFileFormatV1, w.cfg.CaptureID,
				w.cfg.ChangeFeedID.ID,
				w.cfg.LogType, 1, uuidGen.NewString(), redo.LogEXT) + redo.TmpEXT
		} else {
			fileName = fmt.Sprintf(redo.RedoLogFileFormatV2, w.cfg.CaptureID,
				w.cfg.ChangeFeedID.Namespace, w.cfg.ChangeFeedID.ID,
				w.cfg.LogType, 1, uuidGen.NewString(), redo.LogEXT) + redo.TmpEXT
		}
		path := filepath.Join(w.cfg.Dir, fileName)
		info, err := os.Stat(path)
		require.Nil(t, err)
		require.Equal(t, fileName, info.Name())

		w.eventCommitTS.Store(12)
		_, err = w.Write([]byte("tt"))
		require.Nil(t, err)
		w.eventCommitTS.Store(22)
		_, err = w.Write([]byte("t"))
		require.Nil(t, err)

		// after rotate, rename to .log
		if w.cfg.ChangeFeedID.Namespace == model.DefaultNamespace {
			fileName = fmt.Sprintf(redo.RedoLogFileFormatV1, w.cfg.CaptureID,
				w.cfg.ChangeFeedID.ID,
				w.cfg.LogType, 1, uuidGen.NewString(), redo.LogEXT)
		} else {
			fileName = fmt.Sprintf(redo.RedoLogFileFormatV2, w.cfg.CaptureID,
				w.cfg.ChangeFeedID.Namespace, w.cfg.ChangeFeedID.ID,
				w.cfg.LogType, 1, uuidGen.NewString(), redo.LogEXT)
		}
		path = filepath.Join(w.cfg.Dir, fileName)
		info, err = os.Stat(path)
		require.Nil(t, err)
		require.Equal(t, fileName, info.Name())
		// create a .tmp file with first eventCommitTS as name
		if w.cfg.ChangeFeedID.Namespace == model.DefaultNamespace {
			fileName = fmt.Sprintf(redo.RedoLogFileFormatV1, w.cfg.CaptureID,
				w.cfg.ChangeFeedID.ID,
				w.cfg.LogType, 12, uuidGen.NewString(), redo.LogEXT) + redo.TmpEXT
		} else {
			fileName = fmt.Sprintf(redo.RedoLogFileFormatV2, w.cfg.CaptureID,
				w.cfg.ChangeFeedID.Namespace, w.cfg.ChangeFeedID.ID,
				w.cfg.LogType, 12, uuidGen.NewString(), redo.LogEXT) + redo.TmpEXT
		}
		path = filepath.Join(w.cfg.Dir, fileName)
		info, err = os.Stat(path)
		require.Nil(t, err)
		require.Equal(t, fileName, info.Name())
		err = w.Close()
		require.Nil(t, err)
		require.False(t, w.IsRunning())
		// safe close, rename to .log with max eventCommitTS as name
		if w.cfg.ChangeFeedID.Namespace == model.DefaultNamespace {
			fileName = fmt.Sprintf(redo.RedoLogFileFormatV1, w.cfg.CaptureID,
				w.cfg.ChangeFeedID.ID,
				w.cfg.LogType, 22, uuidGen.NewString(), redo.LogEXT)
		} else {
			fileName = fmt.Sprintf(redo.RedoLogFileFormatV2, w.cfg.CaptureID,
				w.cfg.ChangeFeedID.Namespace, w.cfg.ChangeFeedID.ID,
				w.cfg.LogType, 22, uuidGen.NewString(), redo.LogEXT)
		}
		path = filepath.Join(w.cfg.Dir, fileName)
		info, err = os.Stat(path)
		require.Nil(t, err)
		require.Equal(t, fileName, info.Name())

		w1 := &Writer{
			cfg: &writer.LogWriterConfig{
				MaxLogSizeInBytes: 10,
				Dir:               dir,
				ChangeFeedID:      cf11s[idx],
				CaptureID:         "cp",
				LogType:           redo.RedoRowLogFileType,
			},
			uint64buf: make([]byte, 8),
			running:   *atomic.NewBool(true),
			metricWriteBytes: common.RedoWriteBytesGauge.
				WithLabelValues("default", "test-cf11"),
			metricFsyncDuration: common.RedoFsyncDurationHistogram.
				WithLabelValues("default", "test-cf11"),
			metricFlushAllDuration: common.RedoFlushAllDurationHistogram.
				WithLabelValues("default", "test-cf11"),
			uuidGenerator: uuidGen,
		}

		w1.eventCommitTS.Store(1)
		_, err = w1.Write([]byte("tes1t11111"))
		require.Nil(t, err)
		// create a .tmp file
		if w1.cfg.ChangeFeedID.Namespace == model.DefaultNamespace {
			fileName = fmt.Sprintf(redo.RedoLogFileFormatV1, w1.cfg.CaptureID,
				w1.cfg.ChangeFeedID.ID,
				w1.cfg.LogType, 1, uuidGen.NewString(), redo.LogEXT) + redo.TmpEXT
		} else {
			fileName = fmt.Sprintf(redo.RedoLogFileFormatV2, w1.cfg.CaptureID,
				w1.cfg.ChangeFeedID.Namespace, w1.cfg.ChangeFeedID.ID,
				w1.cfg.LogType, 1, uuidGen.NewString(), redo.LogEXT) + redo.TmpEXT
		}
		path = filepath.Join(w1.cfg.Dir, fileName)
		info, err = os.Stat(path)
		require.Nil(t, err)
		require.Equal(t, fileName, info.Name())
		// change the file name, should cause CLose err
		err = os.Rename(path, path+"new")
		require.Nil(t, err)
		err = w1.Close()
		require.NotNil(t, err)
		// closed anyway
		require.False(t, w1.IsRunning())
	}
}

func TestAdvanceTs(t *testing.T) {
	t.Parallel()

	w := &Writer{}
	w.AdvanceTs(111)
	require.EqualValues(t, 111, w.eventCommitTS.Load())
}

func TestNewFileWriter(t *testing.T) {
	t.Parallel()

	_, err := NewFileWriter(context.Background(), nil)
	require.NotNil(t, err)

	storageDir := t.TempDir()
	dir := t.TempDir()

	uuidGen := uuid.NewConstGenerator("const-uuid")
	w, err := NewFileWriter(context.Background(), &writer.LogWriterConfig{
		Dir:                "sdfsf",
		UseExternalStorage: false,
	},
		writer.WithUUIDGenerator(func() uuid.Generator { return uuidGen }),
	)
	require.Nil(t, err)
	backend := &backuppb.StorageBackend{
		Backend: &backuppb.StorageBackend_Local{Local: &backuppb.Local{Path: storageDir}},
	}
	localStorage, err := storage.New(context.Background(), backend, &storage.ExternalStorageOptions{
		SendCredentials: false,
		HTTPClient:      nil,
	})
	w.storage = localStorage
	require.Nil(t, err)
	err = w.Close()
	require.Nil(t, err)
	require.False(t, w.IsRunning())

	controller := gomock.NewController(t)
	mockStorage := mockstorage.NewMockExternalStorage(controller)
	mockStorage.EXPECT().WriteFile(gomock.Any(), "cp_abcd_test_ddl_0_const-uuid.log",
		gomock.Any()).Return(nil).Times(1)

	changefeed := model.ChangeFeedID{
		Namespace: "abcd",
		ID:        "test",
	}
	w = &Writer{
		cfg: &writer.LogWriterConfig{
			Dir:          dir,
			CaptureID:    "cp",
			ChangeFeedID: changefeed,
			LogType:      redo.RedoDDLLogFileType,

			UseExternalStorage: true,
			MaxLogSizeInBytes:  redo.DefaultMaxLogSize * redo.Megabyte,
		},
		uint64buf: make([]byte, 8),
		storage:   mockStorage,
		metricWriteBytes: common.RedoWriteBytesGauge.
			WithLabelValues("default", "test"),
		metricFsyncDuration: common.RedoFsyncDurationHistogram.
			WithLabelValues("default", "test"),
		metricFlushAllDuration: common.RedoFlushAllDurationHistogram.
			WithLabelValues("default", "test"),
		uuidGenerator: uuidGen,
	}
	w.running.Store(true)
	_, err = w.Write([]byte("test"))
	require.Nil(t, err)
	err = w.Flush()
	require.Nil(t, err)

	err = w.Close()
	require.Nil(t, err)
	require.Equal(t, w.running.Load(), false)
}

func TestRotateFileWithFileAllocator(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := NewFileWriter(ctx, nil)
	require.NotNil(t, err)

	controller := gomock.NewController(t)
	mockStorage := mockstorage.NewMockExternalStorage(controller)

	mockStorage.EXPECT().WriteFile(gomock.Any(), "cp_abcd_test_row_0_uuid-1.log",
		gomock.Any()).Return(nil).Times(1)
	mockStorage.EXPECT().WriteFile(gomock.Any(), "cp_abcd_test_row_100_uuid-2.log",
		gomock.Any()).Return(nil).Times(1)

	dir := t.TempDir()
	uuidGen := uuid.NewMock()
	uuidGen.Push("uuid-1")
	uuidGen.Push("uuid-2")
	uuidGen.Push("uuid-3")
	uuidGen.Push("uuid-4")
	uuidGen.Push("uuid-5")
	changefeed := model.ChangeFeedID{
		Namespace: "abcd",
		ID:        "test",
	}
	w := &Writer{
		cfg: &writer.LogWriterConfig{
			Dir:          dir,
			CaptureID:    "cp",
			ChangeFeedID: changefeed,
			LogType:      redo.RedoRowLogFileType,

			UseExternalStorage: true,
			MaxLogSizeInBytes:  redo.DefaultMaxLogSize * redo.Megabyte,
		},
		uint64buf: make([]byte, 8),
		metricWriteBytes: common.RedoWriteBytesGauge.
			WithLabelValues("default", "test"),
		metricFsyncDuration: common.RedoFsyncDurationHistogram.
			WithLabelValues("default", "test"),
		metricFlushAllDuration: common.RedoFlushAllDurationHistogram.
			WithLabelValues("default", "test"),
		storage:       mockStorage,
		uuidGenerator: uuidGen,
	}
	w.allocator = fsutil.NewFileAllocator(
		w.cfg.Dir, redo.RedoRowLogFileType, redo.DefaultMaxLogSize*redo.Megabyte)

	w.running.Store(true)
	_, err = w.Write([]byte("test"))
	require.Nil(t, err)

	err = w.rotate()
	require.Nil(t, err)

	w.AdvanceTs(100)
	_, err = w.Write([]byte("test"))
	require.Nil(t, err)
	err = w.rotate()
	require.Nil(t, err)

	w.Close()
}

func TestRotateFileWithoutFileAllocator(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := NewFileWriter(ctx, nil)
	require.NotNil(t, err)

	controller := gomock.NewController(t)
	mockStorage := mockstorage.NewMockExternalStorage(controller)

	mockStorage.EXPECT().WriteFile(gomock.Any(), "cp_abcd_test_ddl_0_uuid-2.log",
		gomock.Any()).Return(nil).Times(1)
	mockStorage.EXPECT().WriteFile(gomock.Any(), "cp_abcd_test_ddl_100_uuid-4.log",
		gomock.Any()).Return(nil).Times(1)

	dir := t.TempDir()
	uuidGen := uuid.NewMock()
	uuidGen.Push("uuid-1")
	uuidGen.Push("uuid-2")
	uuidGen.Push("uuid-3")
	uuidGen.Push("uuid-4")
	uuidGen.Push("uuid-5")
	uuidGen.Push("uuid-6")
	changefeed := model.ChangeFeedID{
		Namespace: "abcd",
		ID:        "test",
	}
	w := &Writer{
		cfg: &writer.LogWriterConfig{
			Dir:          dir,
			CaptureID:    "cp",
			ChangeFeedID: changefeed,
			LogType:      redo.RedoDDLLogFileType,

			UseExternalStorage: true,
			MaxLogSizeInBytes:  redo.DefaultMaxLogSize * redo.Megabyte,
		},
		uint64buf: make([]byte, 8),
		metricWriteBytes: common.RedoWriteBytesGauge.
			WithLabelValues("default", "test"),
		metricFsyncDuration: common.RedoFsyncDurationHistogram.
			WithLabelValues("default", "test"),
		metricFlushAllDuration: common.RedoFlushAllDurationHistogram.
			WithLabelValues("default", "test"),
		storage:       mockStorage,
		uuidGenerator: uuidGen,
	}
	w.running.Store(true)
	_, err = w.Write([]byte("test"))
	require.Nil(t, err)

	err = w.rotate()
	require.Nil(t, err)

	w.AdvanceTs(100)
	_, err = w.Write([]byte("test"))
	require.Nil(t, err)
	err = w.rotate()
	require.Nil(t, err)

	w.Close()
}
