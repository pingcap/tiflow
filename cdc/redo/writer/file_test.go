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

package writer

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	mockstorage "github.com/pingcap/tidb/br/pkg/mock/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/pkg/uuid"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/atomic"
)

func TestWriterWrite(t *testing.T) {
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
			cfg: &FileWriterConfig{
				MaxLogSize:   10,
				Dir:          dir,
				ChangeFeedID: cf,
				CaptureID:    "cp",
				FileType:     common.DefaultRowLogFileType,
				CreateTime:   time.Date(2000, 1, 1, 1, 1, 1, 1, &time.Location{}),
			},
			uint64buf: make([]byte, 8),
			running:   *atomic.NewBool(true),
			metricWriteBytes: redoWriteBytesGauge.
				WithLabelValues("default", "test-cf"),
			metricFsyncDuration: redoFsyncDurationHistogram.
				WithLabelValues("default", "test-cf"),
			metricFlushAllDuration: redoFlushAllDurationHistogram.
				WithLabelValues("default", "test-cf"),
			uuidGenerator: uuidGen,
		}

		w.eventCommitTS.Store(1)
		_, err := w.Write([]byte("tes1t11111"))
		require.Nil(t, err)
		var fileName string
		// create a .tmp file
		if w.cfg.ChangeFeedID.Namespace == model.DefaultNamespace {
			fileName = fmt.Sprintf(common.RedoLogFileFormatV1, w.cfg.CaptureID,
				w.cfg.ChangeFeedID.ID,
				w.cfg.FileType, 1, uuidGen.NewString(), common.LogEXT) + common.TmpEXT
		} else {
			fileName = fmt.Sprintf(common.RedoLogFileFormatV2, w.cfg.CaptureID,
				w.cfg.ChangeFeedID.Namespace, w.cfg.ChangeFeedID.ID,
				w.cfg.FileType, 1, uuidGen.NewString(), common.LogEXT) + common.TmpEXT
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
			fileName = fmt.Sprintf(common.RedoLogFileFormatV1, w.cfg.CaptureID,
				w.cfg.ChangeFeedID.ID,
				w.cfg.FileType, 1, uuidGen.NewString(), common.LogEXT)
		} else {
			fileName = fmt.Sprintf(common.RedoLogFileFormatV2, w.cfg.CaptureID,
				w.cfg.ChangeFeedID.Namespace, w.cfg.ChangeFeedID.ID,
				w.cfg.FileType, 1, uuidGen.NewString(), common.LogEXT)
		}
		path = filepath.Join(w.cfg.Dir, fileName)
		info, err = os.Stat(path)
		require.Nil(t, err)
		require.Equal(t, fileName, info.Name())
		// create a .tmp file with first eventCommitTS as name
		if w.cfg.ChangeFeedID.Namespace == model.DefaultNamespace {
			fileName = fmt.Sprintf(common.RedoLogFileFormatV1, w.cfg.CaptureID,
				w.cfg.ChangeFeedID.ID,
				w.cfg.FileType, 12, uuidGen.NewString(), common.LogEXT) + common.TmpEXT
		} else {
			fileName = fmt.Sprintf(common.RedoLogFileFormatV2, w.cfg.CaptureID,
				w.cfg.ChangeFeedID.Namespace, w.cfg.ChangeFeedID.ID,
				w.cfg.FileType, 12, uuidGen.NewString(), common.LogEXT) + common.TmpEXT
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
			fileName = fmt.Sprintf(common.RedoLogFileFormatV1, w.cfg.CaptureID,
				w.cfg.ChangeFeedID.ID,
				w.cfg.FileType, 22, uuidGen.NewString(), common.LogEXT)
		} else {
			fileName = fmt.Sprintf(common.RedoLogFileFormatV2, w.cfg.CaptureID,
				w.cfg.ChangeFeedID.Namespace, w.cfg.ChangeFeedID.ID,
				w.cfg.FileType, 22, uuidGen.NewString(), common.LogEXT)
		}
		path = filepath.Join(w.cfg.Dir, fileName)
		info, err = os.Stat(path)
		require.Nil(t, err)
		require.Equal(t, fileName, info.Name())

		w1 := &Writer{
			cfg: &FileWriterConfig{
				MaxLogSize:   10,
				Dir:          dir,
				ChangeFeedID: cf11s[idx],
				CaptureID:    "cp",
				FileType:     common.DefaultRowLogFileType,
				CreateTime:   time.Date(2000, 1, 1, 1, 1, 1, 1, &time.Location{}),
			},
			uint64buf: make([]byte, 8),
			running:   *atomic.NewBool(true),
			metricWriteBytes: redoWriteBytesGauge.
				WithLabelValues("default", "test-cf11"),
			metricFsyncDuration: redoFsyncDurationHistogram.
				WithLabelValues("default", "test-cf11"),
			metricFlushAllDuration: redoFlushAllDurationHistogram.
				WithLabelValues("default", "test-cf11"),
			uuidGenerator: uuidGen,
		}

		w1.eventCommitTS.Store(1)
		_, err = w1.Write([]byte("tes1t11111"))
		require.Nil(t, err)
		// create a .tmp file
		if w1.cfg.ChangeFeedID.Namespace == model.DefaultNamespace {
			fileName = fmt.Sprintf(common.RedoLogFileFormatV1, w1.cfg.CaptureID,
				w1.cfg.ChangeFeedID.ID,
				w1.cfg.FileType, 1, uuidGen.NewString(), common.LogEXT) + common.TmpEXT
		} else {
			fileName = fmt.Sprintf(common.RedoLogFileFormatV2, w1.cfg.CaptureID,
				w1.cfg.ChangeFeedID.Namespace, w1.cfg.ChangeFeedID.ID,
				w1.cfg.FileType, 1, uuidGen.NewString(), common.LogEXT) + common.TmpEXT
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

func TestWriterGC(t *testing.T) {
	dir := t.TempDir()

	uuidGen := uuid.NewConstGenerator("const-uuid")
	controller := gomock.NewController(t)
	mockStorage := mockstorage.NewMockExternalStorage(controller)
	mockStorage.EXPECT().WriteFile(gomock.Any(), "cp_test_row_1_const-uuid.log.tmp",
		gomock.Any()).Return(nil).Times(1)
	mockStorage.EXPECT().WriteFile(gomock.Any(), "cp_test_row_1_const-uuid.log",
		gomock.Any()).Return(nil).Times(1)
	mockStorage.EXPECT().DeleteFile(gomock.Any(), "cp_test_row_1_const-uuid.log.tmp").
		Return(nil).Times(1)

	mockStorage.EXPECT().WriteFile(gomock.Any(), "cp_test_row_2_const-uuid.log.tmp",
		gomock.Any()).Return(nil).Times(1)
	mockStorage.EXPECT().WriteFile(gomock.Any(), "cp_test_row_2_const-uuid.log",
		gomock.Any()).Return(nil).Times(1)
	mockStorage.EXPECT().DeleteFile(gomock.Any(), "cp_test_row_2_const-uuid.log.tmp").
		Return(nil).Times(1)

	mockStorage.EXPECT().WriteFile(gomock.Any(), "cp_test_row_3_const-uuid.log.tmp",
		gomock.Any()).Return(nil).Times(1)
	mockStorage.EXPECT().WriteFile(gomock.Any(), "cp_test_row_3_const-uuid.log",
		gomock.Any()).Return(nil).Times(1)
	mockStorage.EXPECT().DeleteFile(gomock.Any(), "cp_test_row_3_const-uuid.log.tmp").
		Return(nil).Times(1)

	mockStorage.EXPECT().DeleteFile(gomock.Any(), "cp_test_row_1_const-uuid.log").
		Return(errors.New("ignore err")).Times(1)
	mockStorage.EXPECT().DeleteFile(gomock.Any(), "cp_test_row_2_const-uuid.log").
		Return(errors.New("ignore err")).Times(1)

	megabyte = 1
	cfg := &FileWriterConfig{
		Dir:               dir,
		ChangeFeedID:      model.DefaultChangeFeedID("test"),
		CaptureID:         "cp",
		MaxLogSize:        10,
		FileType:          common.DefaultRowLogFileType,
		CreateTime:        time.Date(2000, 1, 1, 1, 1, 1, 1, &time.Location{}),
		FlushIntervalInMs: 5,
		S3Storage:         true,
	}
	w := &Writer{
		cfg:       cfg,
		uint64buf: make([]byte, 8),
		storage:   mockStorage,
		metricWriteBytes: redoWriteBytesGauge.
			WithLabelValues(cfg.ChangeFeedID.Namespace, cfg.ChangeFeedID.ID),
		metricFsyncDuration: redoFsyncDurationHistogram.
			WithLabelValues(cfg.ChangeFeedID.Namespace, cfg.ChangeFeedID.ID),
		metricFlushAllDuration: redoFlushAllDurationHistogram.
			WithLabelValues(cfg.ChangeFeedID.Namespace, cfg.ChangeFeedID.ID),
		uuidGenerator: uuidGen,
	}
	w.running.Store(true)
	w.eventCommitTS.Store(1)
	_, err := w.Write([]byte("t1111"))
	require.Nil(t, err)
	w.eventCommitTS.Store(2)
	_, err = w.Write([]byte("t2222"))
	require.Nil(t, err)
	w.eventCommitTS.Store(3)
	_, err = w.Write([]byte("t3333"))
	require.Nil(t, err)

	files, err := ioutil.ReadDir(w.cfg.Dir)
	require.Nil(t, err)
	require.Equal(t, 3, len(files), "should have 3 log file")

	err = w.GC(3)
	require.Nil(t, err)

	err = w.Close()
	require.Nil(t, err)
	require.False(t, w.IsRunning())
	files, err = ioutil.ReadDir(w.cfg.Dir)
	require.Nil(t, err)
	require.Equal(t, 1, len(files), "should have 1 log left after GC")

	ts, fileType, err := common.ParseLogFileName(files[0].Name())
	require.Nil(t, err, files[0].Name())
	require.EqualValues(t, 3, ts)
	require.Equal(t, common.DefaultRowLogFileType, fileType)
	time.Sleep(time.Duration(100) * time.Millisecond)

	w1 := &Writer{
		cfg:       cfg,
		uint64buf: make([]byte, 8),
		storage:   mockStorage,
	}
	w1.cfg.Dir += "not-exist"
	w1.running.Store(true)
	err = w1.GC(111)
	require.Nil(t, err)
}

func TestAdvanceTs(t *testing.T) {
	w := &Writer{}
	w.AdvanceTs(111)
	require.EqualValues(t, 111, w.eventCommitTS.Load())
}

func TestNewWriter(t *testing.T) {
	_, err := NewWriter(context.Background(), nil)
	require.NotNil(t, err)

	s3URI, err := url.Parse("s3://logbucket/test-changefeed?endpoint=http://111/")
	require.Nil(t, err)

	dir := t.TempDir()

	uuidGen := uuid.NewConstGenerator("const-uuid")
	w, err := NewWriter(context.Background(), &FileWriterConfig{
		Dir:       "sdfsf",
		S3Storage: true,
		S3URI:     *s3URI,
	},
		WithUUIDGenerator(func() uuid.Generator { return uuidGen }),
	)
	require.Nil(t, err)
	time.Sleep(time.Duration(defaultFlushIntervalInMs+1) * time.Millisecond)
	err = w.Close()
	require.Nil(t, err)
	require.False(t, w.IsRunning())

	controller := gomock.NewController(t)
	mockStorage := mockstorage.NewMockExternalStorage(controller)
	mockStorage.EXPECT().WriteFile(gomock.Any(), "cp_abcd_test_ddl_0_const-uuid.log.tmp",
		gomock.Any()).Return(nil).Times(2)
	mockStorage.EXPECT().WriteFile(gomock.Any(), "cp_abcd_test_ddl_0_const-uuid.log",
		gomock.Any()).Return(nil).Times(1)
	mockStorage.EXPECT().DeleteFile(gomock.Any(), "cp_abcd_test_ddl_0_const-uuid.log.tmp").
		Return(nil).Times(1)

	changefeed := model.ChangeFeedID{
		Namespace: "abcd",
		ID:        "test",
	}
	w = &Writer{
		cfg: &FileWriterConfig{
			Dir:          dir,
			CaptureID:    "cp",
			ChangeFeedID: changefeed,
			FileType:     common.DefaultDDLLogFileType,
			CreateTime:   time.Date(2000, 1, 1, 1, 1, 1, 1, &time.Location{}),
			S3Storage:    true,
			MaxLogSize:   defaultMaxLogSize,
		},
		uint64buf: make([]byte, 8),
		storage:   mockStorage,
		metricWriteBytes: redoWriteBytesGauge.
			WithLabelValues("default", "test"),
		metricFsyncDuration: redoFsyncDurationHistogram.
			WithLabelValues("default", "test"),
		metricFlushAllDuration: redoFlushAllDurationHistogram.
			WithLabelValues("default", "test"),
		uuidGenerator: uuidGen,
	}
	w.running.Store(true)
	_, err = w.Write([]byte("test"))
	require.Nil(t, err)
	//
	err = w.Flush()
	require.Nil(t, err)

	err = w.Close()
	require.Nil(t, err)
	require.Equal(t, w.running.Load(), false)
	time.Sleep(time.Duration(defaultFlushIntervalInMs+1) * time.Millisecond)
}

func TestRotateFile(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := NewWriter(ctx, nil)
	require.NotNil(t, err)

	controller := gomock.NewController(t)
	mockStorage := mockstorage.NewMockExternalStorage(controller)

	mockStorage.EXPECT().WriteFile(gomock.Any(), "cp_abcd_test_row_0_uuid-1.log.tmp",
		gomock.Any()).Return(nil).Times(1)
	mockStorage.EXPECT().WriteFile(gomock.Any(), "cp_abcd_test_row_0_uuid-2.log",
		gomock.Any()).Return(nil).Times(1)
	mockStorage.EXPECT().DeleteFile(gomock.Any(), "cp_abcd_test_row_0_uuid-1.log.tmp").
		Return(nil).Times(1)

	mockStorage.EXPECT().WriteFile(gomock.Any(), "cp_abcd_test_row_0_uuid-3.log.tmp",
		gomock.Any()).Return(nil).Times(1)
	mockStorage.EXPECT().WriteFile(gomock.Any(), "cp_abcd_test_row_100_uuid-4.log",
		gomock.Any()).Return(nil).Times(1)
	mockStorage.EXPECT().DeleteFile(gomock.Any(), "cp_abcd_test_row_0_uuid-3.log.tmp").
		Return(nil).Times(1)

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
		cfg: &FileWriterConfig{
			Dir:          dir,
			CaptureID:    "cp",
			ChangeFeedID: changefeed,
			FileType:     common.DefaultRowLogFileType,
			CreateTime:   time.Date(2000, 1, 1, 1, 1, 1, 1, &time.Location{}),
			S3Storage:    true,
			MaxLogSize:   defaultMaxLogSize,
		},
		uint64buf: make([]byte, 8),
		metricWriteBytes: redoWriteBytesGauge.
			WithLabelValues("default", "test"),
		metricFsyncDuration: redoFsyncDurationHistogram.
			WithLabelValues("default", "test"),
		metricFlushAllDuration: redoFlushAllDurationHistogram.
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
}
