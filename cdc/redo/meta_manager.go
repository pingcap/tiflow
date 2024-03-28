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
	"path/filepath"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/pingcap/tiflow/pkg/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var _ MetaManager = (*metaManager)(nil)

// MetaManager defines an interface that is used to manage redo meta and gc logs in owner.
type MetaManager interface {
	redoManager
	// UpdateMeta updates the checkpointTs and resolvedTs asynchronously.
	UpdateMeta(checkpointTs, resolvedTs model.Ts)
	// GetFlushedMeta returns the flushed meta.
	GetFlushedMeta() common.LogMeta
	// Cleanup deletes all redo logs, which are only called from the owner
	// when changefeed is deleted.
	Cleanup(ctx context.Context) error

	// Running return true if the meta manager is running or not.
	Running() bool
}

type metaManager struct {
	captureID    model.CaptureID
	changeFeedID model.ChangeFeedID
	enabled      bool

	// running means the meta manager now running normally.
	running atomic.Bool

	metaCheckpointTs statefulRts
	metaResolvedTs   statefulRts

	// This fields are used to process meta files and perform
	// garbage collection of logs.
	extStorage    storage.ExternalStorage
	uuidGenerator uuid.Generator
	preMetaFile   string

	startTs model.Ts

	lastFlushTime          time.Time
	cfg                    *config.ConsistentConfig
	metricFlushLogDuration prometheus.Observer

	flushIntervalInMs int64
}

// NewDisabledMetaManager creates a disabled Meta Manager.
func NewDisabledMetaManager() *metaManager {
	return &metaManager{
		enabled: false,
	}
}

// NewMetaManager creates a new meta Manager.
func NewMetaManager(
	changefeedID model.ChangeFeedID, cfg *config.ConsistentConfig, checkpoint model.Ts,
) *metaManager {
	// return a disabled Manager if no consistent config or normal consistent level
	if cfg == nil || !redo.IsConsistentEnabled(cfg.Level) {
		return &metaManager{enabled: false}
	}

	m := &metaManager{
		captureID:         config.GetGlobalServerConfig().AdvertiseAddr,
		changeFeedID:      changefeedID,
		uuidGenerator:     uuid.NewGenerator(),
		enabled:           true,
		cfg:               cfg,
		startTs:           checkpoint,
		flushIntervalInMs: cfg.MetaFlushIntervalInMs,
	}

	if m.flushIntervalInMs < redo.MinFlushIntervalInMs {
		log.Warn("redo flush interval is too small, use default value",
			zap.String("namespace", m.changeFeedID.Namespace),
			zap.String("changefeed", m.changeFeedID.ID),
			zap.Int64("interval", m.flushIntervalInMs))
		m.flushIntervalInMs = redo.DefaultMetaFlushIntervalInMs
	}
	return m
}

// Enabled returns whether this log manager is enabled
func (m *metaManager) Enabled() bool {
	return m.enabled
}

// Running return whether the meta manager is initialized,
// which means the external storage is accessible to the meta manager.
func (m *metaManager) Running() bool {
	return m.running.Load()
}

func (m *metaManager) preStart(ctx context.Context) error {
	uri, err := storage.ParseRawURL(m.cfg.Storage)
	if err != nil {
		return err
	}
	// "nfs" and "local" scheme are converted to "file" scheme
	redo.FixLocalScheme(uri)
	// blackhole scheme is converted to "noop" scheme here, so we can use blackhole for testing
	if redo.IsBlackholeStorage(uri.Scheme) {
		uri, _ = storage.ParseRawURL("noop://")
	}

	extStorage, err := redo.InitExternalStorage(ctx, *uri)
	if err != nil {
		return err
	}
	m.extStorage = extStorage

	m.metricFlushLogDuration = common.RedoFlushLogDurationHistogram.
		WithLabelValues(m.changeFeedID.Namespace, m.changeFeedID.ID)

	err = m.preCleanupExtStorage(ctx)
	if err != nil {
		log.Warn("redo: pre clean redo logs fail",
			zap.String("namespace", m.changeFeedID.Namespace),
			zap.String("changefeed", m.changeFeedID.ID),
			zap.Error(err))
		return err
	}
	err = m.initMeta(ctx)
	if err != nil {
		log.Warn("redo: init redo meta fail",
			zap.String("namespace", m.changeFeedID.Namespace),
			zap.String("changefeed", m.changeFeedID.ID),
			zap.Error(err))
		return err
	}
	return nil
}

// Run runs bgFlushMeta and bgGC.
func (m *metaManager) Run(ctx context.Context, _ ...chan<- error) error {
	if err := m.preStart(ctx); err != nil {
		return err
	}
	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return m.bgFlushMeta(egCtx)
	})
	eg.Go(func() error {
		return m.bgGC(egCtx)
	})

	m.running.Store(true)
	return eg.Wait()
}

func (m *metaManager) WaitForReady(_ context.Context) {}

func (m *metaManager) Close() {}

// UpdateMeta updates meta.
func (m *metaManager) UpdateMeta(checkpointTs, resolvedTs model.Ts) {
	if ok := m.metaResolvedTs.checkAndSetUnflushed(resolvedTs); !ok {
		log.Warn("update redo meta with a regressed resolved ts, ignore",
			zap.Uint64("currResolvedTs", m.metaResolvedTs.getFlushed()),
			zap.Uint64("recvResolvedTs", resolvedTs),
			zap.String("namespace", m.changeFeedID.Namespace),
			zap.String("changefeed", m.changeFeedID.ID))
	}
	if ok := m.metaCheckpointTs.checkAndSetUnflushed(checkpointTs); !ok {
		log.Warn("update redo meta with a regressed checkpoint ts, ignore",
			zap.Uint64("currCheckpointTs", m.metaCheckpointTs.getFlushed()),
			zap.Uint64("recvCheckpointTs", checkpointTs),
			zap.String("namespace", m.changeFeedID.Namespace),
			zap.String("changefeed", m.changeFeedID.ID))
	}
}

// GetFlushedMeta gets flushed meta.
func (m *metaManager) GetFlushedMeta() common.LogMeta {
	checkpointTs := m.metaCheckpointTs.getFlushed()
	resolvedTs := m.metaResolvedTs.getFlushed()
	return common.LogMeta{CheckpointTs: checkpointTs, ResolvedTs: resolvedTs}
}

// initMeta will read the meta file from external storage and
// use it to initialize the meta field of the metaManager.
func (m *metaManager) initMeta(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	metas := []*common.LogMeta{
		{CheckpointTs: m.startTs, ResolvedTs: m.startTs},
	}
	var toRemoveMetaFiles []string
	err := m.extStorage.WalkDir(ctx, nil, func(path string, size int64) error {
		log.Info("redo: meta manager walk dir",
			zap.String("namespace", m.changeFeedID.Namespace),
			zap.String("changefeed", m.changeFeedID.ID),
			zap.String("path", path), zap.Int64("size", size))
		// TODO: use prefix to accelerate traverse operation
		if !strings.HasSuffix(path, redo.MetaEXT) {
			return nil
		}
		toRemoveMetaFiles = append(toRemoveMetaFiles, path)

		data, err := m.extStorage.ReadFile(ctx, path)
		if err != nil {
			log.Warn("redo: read meta file failed",
				zap.String("namespace", m.changeFeedID.Namespace),
				zap.String("changefeed", m.changeFeedID.ID),
				zap.String("path", path), zap.Error(err))
			if !util.IsNotExistInExtStorage(err) {
				return err
			}
			return nil
		}
		var meta common.LogMeta
		_, err = meta.UnmarshalMsg(data)
		if err != nil {
			log.Error("redo: unmarshal meta data failed",
				zap.String("namespace", m.changeFeedID.Namespace),
				zap.String("changefeed", m.changeFeedID.ID),
				zap.Error(err), zap.ByteString("data", data))
			return err
		}
		metas = append(metas, &meta)
		return nil
	})
	if err != nil {
		return errors.WrapError(errors.ErrRedoMetaInitialize, err)
	}

	var checkpointTs, resolvedTs uint64
	common.ParseMeta(metas, &checkpointTs, &resolvedTs)
	if checkpointTs == 0 || resolvedTs == 0 {
		log.Panic("checkpointTs or resolvedTs is 0 when initializing redo meta in owner",
			zap.String("namespace", m.changeFeedID.Namespace),
			zap.String("changefeed", m.changeFeedID.ID),
			zap.Uint64("checkpointTs", checkpointTs),
			zap.Uint64("resolvedTs", resolvedTs))
	}
	m.metaResolvedTs.unflushed.Store(resolvedTs)
	m.metaCheckpointTs.unflushed.Store(checkpointTs)
	if err := m.maybeFlushMeta(ctx); err != nil {
		return errors.WrapError(errors.ErrRedoMetaInitialize, err)
	}

	flushedMeta := m.GetFlushedMeta()
	log.Info("redo: meta manager flush init meta success",
		zap.String("namespace", m.changeFeedID.Namespace),
		zap.String("changefeed", m.changeFeedID.ID),
		zap.Uint64("checkpointTs", flushedMeta.CheckpointTs),
		zap.Uint64("resolvedTs", flushedMeta.ResolvedTs))

	return util.DeleteFilesInExtStorage(ctx, m.extStorage, toRemoveMetaFiles)
}

func (m *metaManager) preCleanupExtStorage(ctx context.Context) error {
	deleteMarker := getDeletedChangefeedMarker(m.changeFeedID)
	ret, err := m.extStorage.FileExists(ctx, deleteMarker)
	if err != nil {
		return errors.WrapError(errors.ErrExternalStorageAPI, err)
	}
	if !ret {
		return nil
	}

	changefeedMatcher := getChangefeedMatcher(m.changeFeedID)
	err = util.RemoveFilesIf(ctx, m.extStorage, func(path string) bool {
		if path == deleteMarker || !strings.Contains(path, changefeedMatcher) {
			return false
		}
		return true
	}, nil)
	if err != nil {
		return err
	}

	err = m.extStorage.DeleteFile(ctx, deleteMarker)
	if err != nil && !util.IsNotExistInExtStorage(err) {
		return errors.WrapError(errors.ErrExternalStorageAPI, err)
	}

	return nil
}

// shouldRemoved remove the file which maxCommitTs in file name less than checkPointTs, since
// all event ts < checkPointTs already sent to sink, the log is not needed any more for recovery
func (m *metaManager) shouldRemoved(path string, checkPointTs uint64) bool {
	changefeedMatcher := getChangefeedMatcher(m.changeFeedID)
	if !strings.Contains(path, changefeedMatcher) {
		return false
	}
	if filepath.Ext(path) != redo.LogEXT {
		return false
	}

	commitTs, fileType, err := redo.ParseLogFileName(path)
	if err != nil {
		log.Error("parse file name failed",
			zap.String("namespace", m.changeFeedID.Namespace),
			zap.String("changefeed", m.changeFeedID.ID),
			zap.String("path", path), zap.Error(err))
		return false
	}
	if fileType != redo.RedoDDLLogFileType && fileType != redo.RedoRowLogFileType {
		log.Panic("unknown file type",
			zap.String("namespace", m.changeFeedID.Namespace),
			zap.String("changefeed", m.changeFeedID.ID),
			zap.String("path", path), zap.Any("fileType", fileType))
	}

	// if commitTs == checkPointTs, the DDL may be executed in the owner,
	// so we should not delete it.
	return commitTs < checkPointTs
}

// deleteAllLogs delete all redo logs and leave a deleted mark.
func (m *metaManager) deleteAllLogs(ctx context.Context) error {
	// when one changefeed with redo enabled gets deleted, it's extStorage should always be set to not nil
	// otherwise it should have already meet panic during changefeed running time.
	// the extStorage may be nil in the unit test, so just set the external storage to make unit test happy.
	if m.extStorage == nil {
		uri, err := storage.ParseRawURL(m.cfg.Storage)
		redo.FixLocalScheme(uri)
		if err != nil {
			return err
		}
		m.extStorage, err = redo.InitExternalStorage(ctx, *uri)
		if err != nil {
			return err
		}
	}
	// Write deleted mark before clean any files.
	deleteMarker := getDeletedChangefeedMarker(m.changeFeedID)
	if err := m.extStorage.WriteFile(ctx, deleteMarker, []byte("D")); err != nil {
		return errors.WrapError(errors.ErrExternalStorageAPI, err)
	}
	log.Info("redo manager write deleted mark",
		zap.String("namespace", m.changeFeedID.Namespace),
		zap.String("changefeed", m.changeFeedID.ID))

	changefeedMatcher := getChangefeedMatcher(m.changeFeedID)
	return util.RemoveFilesIf(ctx, m.extStorage, func(path string) bool {
		if path == deleteMarker || !strings.Contains(path, changefeedMatcher) {
			return false
		}
		return true
	}, nil)
}

func (m *metaManager) maybeFlushMeta(ctx context.Context) error {
	hasChange, unflushed := m.prepareForFlushMeta()
	if !hasChange {
		// check stuck
		if time.Since(m.lastFlushTime) > redo.FlushWarnDuration {
			log.Warn("Redo meta has not changed for a long time, owner may be stuck",
				zap.String("namespace", m.changeFeedID.Namespace),
				zap.String("changefeed", m.changeFeedID.ID),
				zap.Duration("lastFlushTime", time.Since(m.lastFlushTime)),
				zap.Any("meta", unflushed))
		}
		return nil
	}

	log.Debug("Flush redo meta",
		zap.String("namespace", m.changeFeedID.Namespace),
		zap.String("changefeed", m.changeFeedID.ID),
		zap.Any("meta", unflushed))
	if err := m.flush(ctx, unflushed); err != nil {
		return err
	}
	m.postFlushMeta(unflushed)
	m.lastFlushTime = time.Now()
	return nil
}

func (m *metaManager) prepareForFlushMeta() (bool, common.LogMeta) {
	flushed := common.LogMeta{}
	flushed.CheckpointTs = m.metaCheckpointTs.getFlushed()
	flushed.ResolvedTs = m.metaResolvedTs.getFlushed()

	unflushed := common.LogMeta{}
	unflushed.CheckpointTs = m.metaCheckpointTs.getUnflushed()
	unflushed.ResolvedTs = m.metaResolvedTs.getUnflushed()

	hasChange := false
	if flushed.CheckpointTs < unflushed.CheckpointTs ||
		flushed.ResolvedTs < unflushed.ResolvedTs {
		hasChange = true
	}
	return hasChange, unflushed
}

func (m *metaManager) postFlushMeta(meta common.LogMeta) {
	m.metaResolvedTs.checkAndSetFlushed(meta.ResolvedTs)
	m.metaCheckpointTs.checkAndSetFlushed(meta.CheckpointTs)
}

func (m *metaManager) flush(ctx context.Context, meta common.LogMeta) error {
	start := time.Now()
	data, err := meta.MarshalMsg(nil)
	if err != nil {
		return errors.WrapError(errors.ErrMarshalFailed, err)
	}
	metaFile := getMetafileName(m.captureID, m.changeFeedID, m.uuidGenerator)
	if err := m.extStorage.WriteFile(ctx, metaFile, data); err != nil {
		log.Error("redo: meta manager flush meta write file failed",
			zap.String("namespace", m.changeFeedID.Namespace),
			zap.String("changefeed", m.changeFeedID.ID),
			zap.Error(err))
		return errors.WrapError(errors.ErrExternalStorageAPI, err)
	}

	if m.preMetaFile != "" {
		if m.preMetaFile == metaFile {
			// This should only happen when use a constant uuid generator in test.
			return nil
		}
		err := m.extStorage.DeleteFile(ctx, m.preMetaFile)
		if err != nil && !util.IsNotExistInExtStorage(err) {
			log.Error("redo: meta manager flush meta delete file failed",
				zap.String("namespace", m.changeFeedID.Namespace),
				zap.String("changefeed", m.changeFeedID.ID),
				zap.Error(err))
			return errors.WrapError(errors.ErrExternalStorageAPI, err)
		}
	}
	m.preMetaFile = metaFile

	log.Debug("flush meta to s3",
		zap.String("namespace", m.changeFeedID.Namespace),
		zap.String("changefeed", m.changeFeedID.ID),
		zap.String("metaFile", metaFile),
		zap.Any("cost", time.Since(start).Milliseconds()))
	m.metricFlushLogDuration.Observe(time.Since(start).Seconds())
	return nil
}

// Cleanup removes all redo logs of this manager, it is called when changefeed is removed
// only owner should call this method.
func (m *metaManager) Cleanup(ctx context.Context) error {
	common.RedoWriteLogDurationHistogram.
		DeleteLabelValues(m.changeFeedID.Namespace, m.changeFeedID.ID)
	common.RedoFlushLogDurationHistogram.
		DeleteLabelValues(m.changeFeedID.Namespace, m.changeFeedID.ID)
	common.RedoTotalRowsCountGauge.
		DeleteLabelValues(m.changeFeedID.Namespace, m.changeFeedID.ID)
	common.RedoWorkerBusyRatio.
		DeleteLabelValues(m.changeFeedID.Namespace, m.changeFeedID.ID)
	return m.deleteAllLogs(ctx)
}

func (m *metaManager) bgFlushMeta(egCtx context.Context) (err error) {
	ticker := time.NewTicker(time.Duration(m.flushIntervalInMs) * time.Millisecond)
	defer func() {
		ticker.Stop()
		log.Info("redo metaManager bgFlushMeta exits",
			zap.String("namespace", m.changeFeedID.Namespace),
			zap.String("changefeed", m.changeFeedID.ID),
			zap.Error(err))
	}()

	m.lastFlushTime = time.Now()
	for {
		select {
		case <-egCtx.Done():
			return errors.Trace(egCtx.Err())
		case <-ticker.C:
			if err := m.maybeFlushMeta(egCtx); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

// bgGC cleans stale files before the flushed checkpoint in background.
func (m *metaManager) bgGC(egCtx context.Context) error {
	ticker := time.NewTicker(time.Duration(redo.DefaultGCIntervalInMs) * time.Millisecond)
	defer ticker.Stop()

	preCkpt := uint64(0)
	for {
		select {
		case <-egCtx.Done():
			log.Info("redo manager GC exits as context cancelled",
				zap.String("namespace", m.changeFeedID.Namespace),
				zap.String("changefeed", m.changeFeedID.ID))
			return errors.Trace(egCtx.Err())
		case <-ticker.C:
			ckpt := m.metaCheckpointTs.getFlushed()
			if ckpt == preCkpt {
				continue
			}
			preCkpt = ckpt
			log.Debug("redo manager GC is triggered",
				zap.Uint64("checkpointTs", ckpt),
				zap.String("namespace", m.changeFeedID.Namespace),
				zap.String("changefeed", m.changeFeedID.ID))
			err := util.RemoveFilesIf(egCtx, m.extStorage, func(path string) bool {
				return m.shouldRemoved(path, ckpt)
			}, nil)
			if err != nil {
				log.Warn("redo manager log GC fail",
					zap.String("namespace", m.changeFeedID.Namespace),
					zap.String("changefeed", m.changeFeedID.ID), zap.Error(err))
				return errors.Trace(err)
			}
		}
	}
}

func getMetafileName(
	captureID model.CaptureID,
	changeFeedID model.ChangeFeedID,
	uuidGenerator uuid.Generator,
) string {
	return fmt.Sprintf(redo.RedoMetaFileFormat, captureID,
		changeFeedID.Namespace, changeFeedID.ID,
		redo.RedoMetaFileType, uuidGenerator.NewString(), redo.MetaEXT)
}

func getChangefeedMatcher(changeFeedID model.ChangeFeedID) string {
	if changeFeedID.Namespace == "default" {
		return fmt.Sprintf("_%s_", changeFeedID.ID)
	}
	return fmt.Sprintf("_%s_%s_", changeFeedID.Namespace, changeFeedID.ID)
}

func getDeletedChangefeedMarker(changeFeedID model.ChangeFeedID) string {
	if changeFeedID.Namespace == model.DefaultNamespace {
		return fmt.Sprintf("delete_%s", changeFeedID.ID)
	}
	return fmt.Sprintf("delete_%s_%s", changeFeedID.Namespace, changeFeedID.ID)
}
