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

package redo

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/pingcap/tiflow/pkg/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestInitAndWriteMeta(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	captureID := "test-capture"
	ctx = contextutil.PutCaptureAddrInCtx(ctx, captureID)
	changefeedID := model.DefaultChangeFeedID("test-changefeed")
	ctx = contextutil.PutChangefeedIDInCtx(ctx, changefeedID)

	extStorage, uri, err := util.GetTestExtStorage(ctx, t.TempDir())
	require.NoError(t, err)

	// write some meta and log files
	metas := []common.LogMeta{
		{CheckpointTs: 1, ResolvedTs: 2},
		{CheckpointTs: 8, ResolvedTs: 9},
		{CheckpointTs: 9, ResolvedTs: 11},
	}

	var toRemoveFiles []string
	for _, meta := range metas {
		data, err := meta.MarshalMsg(nil)
		require.NoError(t, err)
		metaName := getMetafileName(captureID, changefeedID, uuid.NewGenerator())
		err = extStorage.WriteFile(ctx, metaName, data)
		require.NoError(t, err)
		toRemoveFiles = append(toRemoveFiles, metaName)
	}

	var notRemoveFiles []string
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		fileName := "dummy" + getChangefeedMatcher(changefeedID) + strconv.Itoa(i)
		err = extStorage.WriteFile(ctx, fileName, []byte{})
		require.NoError(t, err)
		notRemoveFiles = append(notRemoveFiles, fileName)
	}

	startTs := uint64(10)
	cfg := &config.ConsistentConfig{
		Level:                 string(redo.ConsistentLevelEventual),
		MaxLogSize:            redo.DefaultMaxLogSize,
		Storage:               uri.String(),
		FlushIntervalInMs:     redo.MinFlushIntervalInMs,
		MetaFlushIntervalInMs: redo.MinFlushIntervalInMs,
		EncodingWorkerNum:     redo.DefaultEncodingWorkerNum,
		FlushWorkerNum:        redo.DefaultFlushWorkerNum,
	}
	m := NewMetaManager(changefeedID, cfg, startTs)

	var eg errgroup.Group
	eg.Go(func() error {
		return m.Run(ctx)
	})

	require.Eventually(t, func() bool {
		return startTs == m.metaCheckpointTs.getFlushed()
	}, time.Second, 50*time.Millisecond)

	require.Eventually(t, func() bool {
		return uint64(11) == m.metaResolvedTs.getFlushed()
	}, time.Second, 50*time.Millisecond)

	for _, fileName := range toRemoveFiles {
		ret, err := extStorage.FileExists(ctx, fileName)
		require.NoError(t, err)
		require.False(t, ret, "file %s should be removed", fileName)
	}
	for _, fileName := range notRemoveFiles {
		ret, err := extStorage.FileExists(ctx, fileName)
		require.NoError(t, err)
		require.True(t, ret, "file %s should not be removed", fileName)
	}

	testWriteMeta(ctx, t, m)

	cancel()
	require.ErrorIs(t, eg.Wait(), context.Canceled)
}

func TestPreCleanupAndWriteMeta(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	captureID := "test-capture"
	ctx = contextutil.PutCaptureAddrInCtx(ctx, captureID)
	changefeedID := model.DefaultChangeFeedID("test-changefeed")
	ctx = contextutil.PutChangefeedIDInCtx(ctx, changefeedID)

	extStorage, uri, err := util.GetTestExtStorage(ctx, t.TempDir())
	require.NoError(t, err)

	// write some meta and log files
	metas := []common.LogMeta{
		{CheckpointTs: 1, ResolvedTs: 2},
		{CheckpointTs: 8, ResolvedTs: 9},
		{CheckpointTs: 9, ResolvedTs: 11},
		{CheckpointTs: 11, ResolvedTs: 12},
	}
	var toRemoveFiles []string
	for _, meta := range metas {
		data, err := meta.MarshalMsg(nil)
		require.NoError(t, err)
		metaName := getMetafileName(captureID, changefeedID, uuid.NewGenerator())
		err = extStorage.WriteFile(ctx, metaName, data)
		require.NoError(t, err)
		toRemoveFiles = append(toRemoveFiles, metaName)
	}
	// write delete mark to simulate a deleted changefeed
	err = extStorage.WriteFile(ctx, getDeletedChangefeedMarker(changefeedID), []byte{})
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		fileName := "dummy" + getChangefeedMatcher(changefeedID) + strconv.Itoa(i)
		err = extStorage.WriteFile(ctx, fileName, []byte{})
		require.NoError(t, err)
		toRemoveFiles = append(toRemoveFiles, fileName)
	}

	startTs := uint64(10)
	cfg := &config.ConsistentConfig{
		Level:                 string(redo.ConsistentLevelEventual),
		MaxLogSize:            redo.DefaultMaxLogSize,
		Storage:               uri.String(),
		FlushIntervalInMs:     redo.MinFlushIntervalInMs,
		MetaFlushIntervalInMs: redo.MinFlushIntervalInMs,
		EncodingWorkerNum:     redo.DefaultEncodingWorkerNum,
		FlushWorkerNum:        redo.DefaultFlushWorkerNum,
	}
	m := NewMetaManager(changefeedID, cfg, startTs)

	var eg errgroup.Group
	eg.Go(func() error {
		return m.Run(ctx)
	})

	require.Eventually(t, func() bool {
		return startTs == m.metaCheckpointTs.getFlushed()
	}, time.Second, 50*time.Millisecond)

	require.Eventually(t, func() bool {
		return startTs == m.metaResolvedTs.getFlushed()
	}, time.Second, 50*time.Millisecond)

	for _, fileName := range toRemoveFiles {
		ret, err := extStorage.FileExists(ctx, fileName)
		require.NoError(t, err)
		require.False(t, ret, "file %s should be removed", fileName)
	}
	testWriteMeta(ctx, t, m)

	cancel()
	require.ErrorIs(t, eg.Wait(), context.Canceled)
}

func testWriteMeta(ctx context.Context, t *testing.T, m *metaManager) {
	checkMeta := func(targetCheckpointTs, targetResolvedTs uint64) {
		var checkpointTs, resolvedTs uint64
		var metas []*common.LogMeta
		cnt := 0
		m.extStorage.WalkDir(ctx, nil, func(path string, size int64) error {
			if !strings.HasSuffix(path, redo.MetaEXT) {
				return nil
			}
			cnt++
			data, err := m.extStorage.ReadFile(ctx, path)
			require.NoError(t, err)
			meta := &common.LogMeta{}
			_, err = meta.UnmarshalMsg(data)
			require.NoError(t, err)
			metas = append(metas, meta)
			return nil
		})
		require.Equal(t, 1, cnt)
		common.ParseMeta(metas, &checkpointTs, &resolvedTs)
		require.Equal(t, targetCheckpointTs, checkpointTs)
		require.Equal(t, targetResolvedTs, resolvedTs)
	}

	// test both regressed
	meta := m.GetFlushedMeta()
	m.UpdateMeta(1, 2)
	checkMeta(meta.CheckpointTs, meta.ResolvedTs)

	// test checkpoint regressed
	m.UpdateMeta(3, 20)
	require.Eventually(t, func() bool {
		return m.metaResolvedTs.getFlushed() == 20
	}, time.Second, 50*time.Millisecond)
	checkMeta(meta.CheckpointTs, 20)

	// test resolved regressed
	m.UpdateMeta(15, 18)
	require.Eventually(t, func() bool {
		return m.metaCheckpointTs.getFlushed() == 15
	}, time.Second, 50*time.Millisecond)
	checkMeta(15, 20)

	// test both advanced
	m.UpdateMeta(16, 21)
	require.Eventually(t, func() bool {
		return m.metaCheckpointTs.getFlushed() == 16
	}, time.Second, 50*time.Millisecond)
	checkMeta(16, 21)
}

func TestGCAndCleanup(t *testing.T) {
	t.Parallel()

	originValue := redo.DefaultGCIntervalInMs
	redo.DefaultGCIntervalInMs = 20
	defer func() {
		redo.DefaultGCIntervalInMs = originValue
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	captureID := "test-capture"
	ctx = contextutil.PutCaptureAddrInCtx(ctx, captureID)
	changefeedID := model.DefaultChangeFeedID("test-changefeed")
	ctx = contextutil.PutChangefeedIDInCtx(ctx, changefeedID)

	extStorage, uri, err := util.GetTestExtStorage(ctx, t.TempDir())
	require.NoError(t, err)

	checkGC := func(checkpointTs uint64) {
		time.Sleep(time.Duration(redo.DefaultGCIntervalInMs) * time.Millisecond * 20)
		checkEqual := false
		extStorage.WalkDir(ctx, nil, func(path string, size int64) error {
			if strings.HasSuffix(path, redo.MetaEXT) {
				return nil
			}
			commitTs, _, err := redo.ParseLogFileName(path)
			require.NoError(t, err)
			require.LessOrEqual(t, checkpointTs, commitTs)
			if checkpointTs == commitTs {
				checkEqual = true
			}
			return nil
		})
		require.True(t, checkEqual)
	}

	// write some log files
	require.NoError(t, err)
	maxCommitTs := 20
	for i := 1; i <= maxCommitTs; i++ {
		for _, logType := range []string{redo.RedoRowLogFileType, redo.RedoDDLLogFileType} {
			// fileName with different captureID and maxCommitTs
			curCaptureID := fmt.Sprintf("%s%d", captureID, i%10)
			maxCommitTs := i
			fileName := fmt.Sprintf(redo.RedoLogFileFormatV1, curCaptureID, changefeedID.ID,
				logType, maxCommitTs, uuid.NewGenerator().NewString(), redo.LogEXT)
			err := extStorage.WriteFile(ctx, fileName, []byte{})
			require.NoError(t, err)
		}
	}

	startTs := uint64(3)
	cfg := &config.ConsistentConfig{
		Level:                 string(redo.ConsistentLevelEventual),
		MaxLogSize:            redo.DefaultMaxLogSize,
		Storage:               uri.String(),
		FlushIntervalInMs:     redo.MinFlushIntervalInMs,
		MetaFlushIntervalInMs: redo.MinFlushIntervalInMs,
		EncodingWorkerNum:     redo.DefaultEncodingWorkerNum,
		FlushWorkerNum:        redo.DefaultFlushWorkerNum,
	}
	m := NewMetaManager(changefeedID, cfg, startTs)

	var eg errgroup.Group
	eg.Go(func() error {
		return m.Run(ctx)
	})

	require.Eventually(t, func() bool {
		return startTs == m.metaCheckpointTs.getFlushed()
	}, time.Second, 50*time.Millisecond)

	require.Eventually(t, func() bool {
		return startTs == m.metaResolvedTs.getFlushed()
	}, time.Second, 50*time.Millisecond)

	checkGC(startTs)

	for i := startTs; i <= uint64(maxCommitTs); i++ {
		m.UpdateMeta(i, 100)
		checkGC(i)
	}

	cancel()
	require.ErrorIs(t, eg.Wait(), context.Canceled)

	m.Cleanup(ctx)
	ret, err := extStorage.FileExists(ctx, getDeletedChangefeedMarker(changefeedID))
	require.NoError(t, err)
	require.True(t, ret)
	cnt := 0
	extStorage.WalkDir(ctx, nil, func(path string, size int64) error {
		cnt++
		return nil
	})
	require.Equal(t, 1, cnt)
}
