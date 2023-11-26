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

package cloudstorage

import (
	"context"
	"encoding/json"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/robfig/cron"
	"go.uber.org/zap"
)

// Assert Sink implementation
var _ ddlsink.Sink = (*DDLSink)(nil)

// DDLSink is a sink that sends DDL events to the cloud storage system.
type DDLSink struct {
	// id indicates which changefeed this sink belongs to.
	id model.ChangeFeedID
	// statistic is used to record the DDL metrics
	statistics *metrics.Statistics
	storage    storage.ExternalStorage
	cfg        *cloudstorage.Config
	cron       *cron.Cron

	lastCheckpointTs         atomic.Uint64
	lastSendCheckpointTsTime time.Time
}

// NewDDLSink creates a ddl sink for cloud storage.
func NewDDLSink(ctx context.Context,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
) (*DDLSink, error) {
	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)
	return newDDLSink(ctx, changefeedID, sinkURI, replicaConfig, nil)
}

func newDDLSink(ctx context.Context,
	changefeedID model.ChangeFeedID,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
	cleanupJobs []func(), /* only for test */
) (*DDLSink, error) {
	// create cloud storage config and then apply the params of sinkURI to it.
	cfg := cloudstorage.NewConfig()
	err := cfg.Apply(ctx, sinkURI, replicaConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	storage, err := util.GetExternalStorageFromURI(ctx, sinkURI.String())
	if err != nil {
		return nil, err
	}

	d := &DDLSink{
		id:                       changefeedID,
		storage:                  storage,
		statistics:               metrics.NewStatistics(ctx, sink.TxnSink),
		cfg:                      cfg,
		lastSendCheckpointTsTime: time.Now(),
	}

	if err := d.initCron(ctx, sinkURI, cleanupJobs); err != nil {
		return nil, errors.Trace(err)
	}
	// Note: It is intended to run the cleanup goroutine in the background.
	// we don't wait for it to finish since the gourotine would be stuck if
	// the downstream is abnormal, especially when the downstream is a nfs.
	go d.bgCleanup(ctx)
	return d, nil
}

// WriteDDLEvent writes the ddl event to the cloud storage.
func (d *DDLSink) WriteDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	writeFile := func(def cloudstorage.TableDefinition) error {
		encodedDef, err := def.MarshalWithQuery()
		if err != nil {
			return errors.Trace(err)
		}

		path, err := def.GenerateSchemaFilePath()
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("write ddl event to external storage",
			zap.String("path", path), zap.Any("ddl", ddl))
		return d.statistics.RecordDDLExecution(func() error {
			err1 := d.storage.WriteFile(ctx, path, encodedDef)
			if err1 != nil {
				return err1
			}

			return nil
		})
	}

	var def cloudstorage.TableDefinition
	def.FromDDLEvent(ddl)
	if err := writeFile(def); err != nil {
		return errors.Trace(err)
	}

	if ddl.Type == timodel.ActionExchangeTablePartition {
		// For exchange partition, we need to write the schema of the source table.
		var sourceTableDef cloudstorage.TableDefinition
		sourceTableDef.FromTableInfo(ddl.PreTableInfo, ddl.TableInfo.Version)
		return writeFile(sourceTableDef)
	}
	return nil
}

// WriteCheckpointTs writes the checkpoint ts to the cloud storage.
func (d *DDLSink) WriteCheckpointTs(ctx context.Context,
	ts uint64, tables []*model.TableInfo,
) error {
	if time.Since(d.lastSendCheckpointTsTime) < 2*time.Second {
		log.Debug("skip write checkpoint ts to external storage",
			zap.Any("changefeedID", d.id),
			zap.Uint64("ts", ts))
		return nil
	}

	defer func() {
		d.lastSendCheckpointTsTime = time.Now()
		d.lastCheckpointTs.Store(ts)
	}()
	ckpt, err := json.Marshal(map[string]uint64{"checkpoint-ts": ts})
	if err != nil {
		return errors.Trace(err)
	}
	err = d.storage.WriteFile(ctx, "metadata", ckpt)
	return errors.Trace(err)
}

func (d *DDLSink) initCron(
	ctx context.Context, sinkURI *url.URL, cleanupJobs []func(),
) (err error) {
	if cleanupJobs == nil {
		cleanupJobs = d.genCleanupJob(ctx, sinkURI)
	}

	d.cron = cron.New()
	for _, job := range cleanupJobs {
		err = d.cron.AddFunc(d.cfg.FileCleanupCronSpec, job)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *DDLSink) bgCleanup(ctx context.Context) {
	if d.cfg.DateSeparator != config.DateSeparatorDay.String() || d.cfg.FileExpirationDays <= 0 {
		log.Info("skip cleanup expired files for storage sink",
			zap.String("namespace", d.id.Namespace),
			zap.String("changefeedID", d.id.ID),
			zap.String("dateSeparator", d.cfg.DateSeparator),
			zap.Int("expiredFileTTL", d.cfg.FileExpirationDays))
		return
	}

	d.cron.Start()
	defer d.cron.Stop()
	log.Info("start schedule cleanup expired files for storage sink",
		zap.String("namespace", d.id.Namespace),
		zap.String("changefeedID", d.id.ID),
		zap.String("dateSeparator", d.cfg.DateSeparator),
		zap.Int("expiredFileTTL", d.cfg.FileExpirationDays))

	// wait for the context done
	<-ctx.Done()
	log.Info("stop schedule cleanup expired files for storage sink",
		zap.String("namespace", d.id.Namespace),
		zap.String("changefeedID", d.id.ID),
		zap.Error(ctx.Err()))
}

func (d *DDLSink) genCleanupJob(ctx context.Context, uri *url.URL) []func() {
	ret := []func(){}

	isLocal := uri.Scheme == "file" || uri.Scheme == "local" || uri.Scheme == ""
	isRemoveEmptyDirsRuning := atomic.Bool{}
	if isLocal {
		ret = append(ret, func() {
			if !isRemoveEmptyDirsRuning.CompareAndSwap(false, true) {
				log.Warn("remove empty dirs is already running, skip this round",
					zap.String("namespace", d.id.Namespace),
					zap.String("changefeedID", d.id.ID))
				return
			}

			checkpointTs := d.lastCheckpointTs.Load()
			start := time.Now()
			cnt, err := cloudstorage.RemoveEmptyDirs(ctx, d.id, uri.Path)
			if err != nil {
				log.Error("failed to remove empty dirs",
					zap.String("namespace", d.id.Namespace),
					zap.String("changefeedID", d.id.ID),
					zap.Uint64("checkpointTs", checkpointTs),
					zap.Duration("cost", time.Since(start)),
					zap.Error(err),
				)
				return
			}
			log.Info("remove empty dirs",
				zap.String("namespace", d.id.Namespace),
				zap.String("changefeedID", d.id.ID),
				zap.Uint64("checkpointTs", checkpointTs),
				zap.Uint64("count", cnt),
				zap.Duration("cost", time.Since(start)))
		})
	}

	isCleanupRunning := atomic.Bool{}
	ret = append(ret, func() {
		if !isCleanupRunning.CompareAndSwap(false, true) {
			log.Warn("cleanup expired files is already running, skip this round",
				zap.String("namespace", d.id.Namespace),
				zap.String("changefeedID", d.id.ID))
			return
		}

		defer isCleanupRunning.Store(false)
		start := time.Now()
		checkpointTs := d.lastCheckpointTs.Load()
		cnt, err := cloudstorage.RemoveExpiredFiles(ctx, d.id, d.storage, d.cfg, checkpointTs)
		if err != nil {
			log.Error("failed to remove expired files",
				zap.String("namespace", d.id.Namespace),
				zap.String("changefeedID", d.id.ID),
				zap.Uint64("checkpointTs", checkpointTs),
				zap.Duration("cost", time.Since(start)),
				zap.Error(err),
			)
			return
		}
		log.Info("remove expired files",
			zap.String("namespace", d.id.Namespace),
			zap.String("changefeedID", d.id.ID),
			zap.Uint64("checkpointTs", checkpointTs),
			zap.Uint64("count", cnt),
			zap.Duration("cost", time.Since(start)))
	})
	return ret
}

// Close closes the sink.
func (d *DDLSink) Close() {
	if d.statistics != nil {
		d.statistics.Close()
	}
}
