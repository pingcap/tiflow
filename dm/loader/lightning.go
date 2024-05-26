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

package loader

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/dumpling/export"
	lserver "github.com/pingcap/tidb/lightning/pkg/server"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	lcfg "github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	tidbpromutil "github.com/pingcap/tidb/pkg/util/promutil"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/cputil"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/storage"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/unit"
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/prometheus/client_golang/prometheus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	// checkpoint file name for lightning loader
	// this file is used to store the real checkpoint data for lightning.
	lightningCheckpointFileName = "tidb_lightning_checkpoint.pb"
)

// LightningLoader can load your mydumper data into TiDB database.
type LightningLoader struct {
	sync.RWMutex

	timeZone              string
	sqlMode               string
	lightningGlobalConfig *lcfg.GlobalConfig
	cfg                   *config.SubTaskConfig

	checkPointList *LightningCheckpointList

	logger log.Logger
	cli    *clientv3.Client
	core   *lserver.Lightning
	cancel context.CancelFunc // for per task context, which maybe different from lightning context

	toDB *conn.BaseDB

	workerName     string
	finish         atomic.Bool
	closed         atomic.Bool
	metaBinlog     atomic.String
	metaBinlogGTID atomic.String
	lastErr        error

	speedRecorder *export.SpeedRecorder
	metricProxies *metricProxies
}

// NewLightning creates a new Loader importing data with lightning.
func NewLightning(cfg *config.SubTaskConfig, cli *clientv3.Client, workerName string) *LightningLoader {
	lightningCfg := MakeGlobalConfig(cfg)
	logger := log.L()
	if cfg.FrameworkLogger != nil {
		logger = log.Logger{Logger: cfg.FrameworkLogger}
	}
	loader := &LightningLoader{
		cfg:                   cfg,
		cli:                   cli,
		workerName:            workerName,
		lightningGlobalConfig: lightningCfg,
		core:                  lserver.New(lightningCfg),
		logger:                logger.WithFields(zap.String("task", cfg.Name), zap.String("unit", "lightning-load")),
		speedRecorder:         export.NewSpeedRecorder(),
	}
	return loader
}

// MakeGlobalConfig converts subtask config to lightning global config.
func MakeGlobalConfig(cfg *config.SubTaskConfig) *lcfg.GlobalConfig {
	lightningCfg := lcfg.NewGlobalConfig()
	if cfg.To.Security != nil {
		lightningCfg.Security.CABytes = cfg.To.Security.SSLCABytes
		lightningCfg.Security.CertBytes = cfg.To.Security.SSLCertBytes
		lightningCfg.Security.KeyBytes = cfg.To.Security.SSLKeyBytes
	}
	lightningCfg.TiDB.Host = cfg.To.Host
	lightningCfg.TiDB.Psw = cfg.To.Password
	lightningCfg.TiDB.User = cfg.To.User
	lightningCfg.TiDB.Port = cfg.To.Port
	if len(cfg.LoaderConfig.PDAddr) > 0 {
		lightningCfg.TiDB.PdAddr = cfg.LoaderConfig.PDAddr
	}
	lightningCfg.TikvImporter.Backend = lcfg.BackendTiDB
	if cfg.LoaderConfig.ImportMode == config.LoadModePhysical {
		lightningCfg.TikvImporter.Backend = lcfg.BackendLocal
	}
	lightningCfg.PostRestore.Checksum = lcfg.OpLevelOff
	if lightningCfg.TikvImporter.Backend == lcfg.BackendLocal {
		lightningCfg.TikvImporter.SortedKVDir = cfg.SortingDirPhysical
	}
	lightningCfg.Mydumper.SourceDir = cfg.Dir
	lightningCfg.App.Config.File = "" // make lightning not init logger, see more in https://github.com/pingcap/tidb/pull/29291
	return lightningCfg
}

// Type implements Unit.Type.
func (l *LightningLoader) Type() pb.UnitType {
	return pb.UnitType_Load
}

func (l *LightningLoader) initMetricProxies() {
	if l.cfg.MetricsFactory != nil {
		// running inside dataflow-engine and the factory is an auto register/deregister factory
		l.metricProxies = newMetricProxies(l.cfg.MetricsFactory)
	} else {
		l.metricProxies = defaultMetricProxies
	}
}

// Init initializes loader for a load task, but not start Process.
// if fail, it should not call l.Close.
func (l *LightningLoader) Init(ctx context.Context) (err error) {
	l.initMetricProxies()

	l.toDB, err = conn.GetDownstreamDB(&l.cfg.To)
	if err != nil {
		return err
	}

	checkpointList := NewLightningCheckpointList(l.toDB, l.cfg.Name, l.cfg.SourceID, l.cfg.MetaSchema, l.logger)
	err = checkpointList.Prepare(ctx)
	if err == nil {
		l.checkPointList = checkpointList
	}
	failpoint.Inject("ignoreLoadCheckpointErr", func(_ failpoint.Value) {
		l.logger.Info("", zap.String("failpoint", "ignoreLoadCheckpointErr"))
		err = nil
	})
	if err != nil {
		return err
	}

	timeZone := l.cfg.Timezone
	if len(timeZone) == 0 {
		baseDB, err2 := conn.GetDownstreamDB(&l.cfg.To)
		if err2 != nil {
			return err2
		}
		defer baseDB.Close()
		var err1 error
		timeZone, err1 = config.FetchTimeZoneSetting(ctx, baseDB.DB)
		if err1 != nil {
			return err1
		}
	}
	l.timeZone = timeZone

	for k, v := range l.cfg.To.Session {
		if strings.ToLower(k) == "sql_mode" {
			l.sqlMode = v
			break
		}
	}

	if len(l.sqlMode) == 0 {
		sqlModes, err3 := conn.AdjustSQLModeCompatible(l.cfg.LoaderConfig.SQLMode)
		if err3 != nil {
			l.logger.Warn("cannot adjust sql_mode compatible, the sql_mode will stay the same", log.ShortError(err3))
		}
		l.sqlMode = sqlModes
	}

	return nil
}

func (l *LightningLoader) ignoreCheckpointError(ctx context.Context, cfg *lcfg.Config) error {
	status, err := l.checkPointList.taskStatus(ctx)
	if err != nil {
		return err
	}
	if status != lightningStatusRunning {
		return nil
	}
	cpdb, err := checkpoints.OpenCheckpointsDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		_ = cpdb.Close()
	}()
	return errors.Trace(cpdb.IgnoreErrorCheckpoint(ctx, "all"))
}

func (l *LightningLoader) runLightning(ctx context.Context, cfg *lcfg.Config) (err error) {
	taskCtx, cancel := context.WithCancel(ctx)
	l.Lock()
	l.cancel = cancel
	l.Unlock()

	// always try to skill all checkpoint errors so we can resume this phase.
	err = l.ignoreCheckpointError(ctx, cfg)
	if err != nil {
		l.logger.Warn("check lightning checkpoint status failed, skip this error", log.ShortError(err))
	}
	if err = l.checkPointList.UpdateStatus(ctx, lightningStatusRunning); err != nil {
		return err
	}

	var opts []lserver.Option
	if l.cfg.MetricsFactory != nil {
		// this branch means dataflow engine has set a Factory, the Factory itself
		// will register and deregister metrics, but lightning will expect the
		// register and deregister at the beginning and end of its lifetime.
		// So we use dataflow engine's Factory to register, and use dataflow engine's
		// global metrics to manually deregister.
		opts = append(opts,
			lserver.WithPromFactory(
				promutil.NewWrappingFactory(
					l.cfg.MetricsFactory,
					"",
					prometheus.Labels{"task": l.cfg.Name, "source_id": l.cfg.SourceID},
				)),
			lserver.WithPromRegistry(promutil.GetGlobalMetricRegistry()))
	} else {
		registry := prometheus.DefaultGatherer.(prometheus.Registerer)
		failpoint.Inject("DontUnregister", func() {
			registry = promutil.NewOnlyRegRegister(registry)
		})

		opts = append(opts,
			lserver.WithPromFactory(
				promutil.NewWrappingFactory(
					tidbpromutil.NewDefaultFactory(),
					"",
					prometheus.Labels{"task": l.cfg.Name, "source_id": l.cfg.SourceID},
				),
			),
			lserver.WithPromRegistry(registry))
	}
	if l.cfg.ExtStorage != nil {
		opts = append(opts,
			lserver.WithDumpFileStorage(l.cfg.ExtStorage))
	}
	if l.cfg.FrameworkLogger != nil {
		opts = append(opts, lserver.WithLogger(l.cfg.FrameworkLogger))
	} else {
		opts = append(opts, lserver.WithLogger(l.logger.Logger))
	}

	var hasDup atomic.Bool
	if l.cfg.LoaderConfig.ImportMode == config.LoadModePhysical {
		opts = append(opts, lserver.WithDupIndicator(&hasDup))
	}

	err = l.core.RunOnceWithOptions(taskCtx, cfg, opts...)
	failpoint.Inject("LoadDataSlowDown", nil)
	failpoint.Inject("LoadDataSlowDownByTask", func(val failpoint.Value) {
		tasks := val.(string)
		taskNames := strings.Split(tasks, ",")
		for _, taskName := range taskNames {
			if l.cfg.Name == taskName {
				l.logger.Info("inject failpoint LoadDataSlowDownByTask in lightning loader", zap.String("task", taskName))
				<-taskCtx.Done()
			}
		}
	})
	defer func() {
		l.lastErr = err
	}()
	if err != nil {
		return convertLightningError(err)
	}
	if hasDup.Load() {
		return terror.ErrLoadLightningHasDup.Generate(cfg.App.TaskInfoSchemaName, errormanager.ConflictErrorTableName)
	}
	return nil
}

var checksumErrorPattern = regexp.MustCompile(`total_kvs: (\d*) vs (\d*)`)

func convertLightningError(err error) error {
	if common.ErrChecksumMismatch.Equal(err) {
		lErr := errors.Cause(err).(*errors.Error)
		msg := lErr.GetMsg()
		matches := checksumErrorPattern.FindStringSubmatch(msg)
		if len(matches) == 3 {
			return terror.ErrLoadLightningChecksum.Generate(matches[2], matches[1])
		}
	}
	return terror.ErrLoadLightningRuntime.Delegate(err)
}

// GetTaskInfoSchemaName is used to assign to TikvImporter.DuplicateResolution in lightning config.
func GetTaskInfoSchemaName(dmMetaSchema, taskName string) string {
	return dmMetaSchema + "_" + taskName
}

// GetLightningConfig returns the lightning task config for the lightning global config and DM subtask config.
func GetLightningConfig(globalCfg *lcfg.GlobalConfig, subtaskCfg *config.SubTaskConfig) (*lcfg.Config, error) {
	cfg := lcfg.NewConfig()
	if err := cfg.LoadFromGlobal(globalCfg); err != nil {
		return nil, err
	}
	// TableConcurrency is adjusted to the value of RegionConcurrency
	// when using TiDB backend.
	// TODO: should we set the TableConcurrency separately.
	cfg.App.RegionConcurrency = subtaskCfg.LoaderConfig.PoolSize
	cfg.Routes = subtaskCfg.RouteRules

	// Use MySQL checkpoint when we use s3/gcs as dumper storage
	if subtaskCfg.ExtStorage != nil || !storage.IsLocalDiskPath(subtaskCfg.LoaderConfig.Dir) {
		// NOTE: If we use bucket as dumper storage, write lightning checkpoint to downstream DB to avoid bucket ratelimit
		// since we will use check Checkpoint in 'ignoreCheckpointError', MAKE SURE we have assigned the Checkpoint config properly here
		if err := cfg.Security.BuildTLSConfig(); err != nil {
			return nil, err
		}
		// To enable the loader worker failover, we need to use jobID+sourceID to isolate the checkpoint schema
		cfg.Checkpoint.Schema = cputil.LightningCheckpointSchema(subtaskCfg.Name, subtaskCfg.SourceID)
		cfg.Checkpoint.Driver = lcfg.CheckpointDriverMySQL
		cfg.Checkpoint.MySQLParam = connParamFromConfig(cfg)
	} else {
		// NOTE: for op dm, we recommend to keep data files and checkpoint file in the same place to avoid inconsistent deletion
		cfg.Checkpoint.Driver = lcfg.CheckpointDriverFile
		var cpPath string
		// l.cfg.LoaderConfig.Dir may be a s3 path, and Lightning supports checkpoint in s3, we can use storage.AdjustPath to adjust path both local and s3.
		cpPath, err := storage.AdjustPath(subtaskCfg.LoaderConfig.Dir, string(filepath.Separator)+lightningCheckpointFileName)
		if err != nil {
			return nil, err
		}
		cfg.Checkpoint.DSN = cpPath
	}
	// TODO: Fix me. Remove strategy may cause the re-import if the process exits unexpectly between removing lightning
	// checkpoint meta and updating dm checkpoint meta to 'finished'.
	cfg.Checkpoint.KeepAfterSuccess = lcfg.CheckpointRemove

	if subtaskCfg.LoaderConfig.DiskQuotaPhysical > 0 {
		cfg.TikvImporter.DiskQuota = subtaskCfg.LoaderConfig.DiskQuotaPhysical
	}
	if cfg.TikvImporter.Backend == lcfg.BackendLocal {
		cfg.TikvImporter.IncrementalImport = true
	} else if err := cfg.TikvImporter.OnDuplicate.FromStringValue(string(subtaskCfg.OnDuplicateLogical)); err != nil {
		return nil, err
	}
	switch subtaskCfg.OnDuplicatePhysical {
	case config.OnDuplicateManual:
		cfg.TikvImporter.DuplicateResolution = lcfg.ReplaceOnDup
		cfg.App.TaskInfoSchemaName = GetTaskInfoSchemaName(subtaskCfg.MetaSchema, subtaskCfg.Name)
	case config.OnDuplicateNone:
		cfg.TikvImporter.DuplicateResolution = lcfg.NoneOnDup
	}
	switch subtaskCfg.ChecksumPhysical {
	case config.OpLevelRequired:
		cfg.PostRestore.Checksum = lcfg.OpLevelRequired
	case config.OpLevelOptional:
		cfg.PostRestore.Checksum = lcfg.OpLevelOptional
	case config.OpLevelOff:
		cfg.PostRestore.Checksum = lcfg.OpLevelOff
	}
	switch subtaskCfg.Analyze {
	case config.OpLevelRequired:
		cfg.PostRestore.Analyze = lcfg.OpLevelRequired
	case config.OpLevelOptional:
		cfg.PostRestore.Analyze = lcfg.OpLevelOptional
	case config.OpLevelOff:
		cfg.PostRestore.Analyze = lcfg.OpLevelOff
	}
	cfg.TiDB.Vars = make(map[string]string)
	cfg.Routes = subtaskCfg.RouteRules
	if subtaskCfg.To.Session != nil {
		for k, v := range subtaskCfg.To.Session {
			cfg.TiDB.Vars[k] = v
		}
	}

	if subtaskCfg.RangeConcurrency > 0 {
		cfg.TikvImporter.RangeConcurrency = subtaskCfg.RangeConcurrency
	}
	if len(subtaskCfg.CompressKVPairs) > 0 {
		err := cfg.TikvImporter.CompressKVPairs.FromStringValue(subtaskCfg.CompressKVPairs)
		if err != nil {
			return nil, err
		}
	}

	cfg.TiDB.Vars = map[string]string{
		// always set transaction mode to optimistic
		"tidb_txn_mode": "optimistic",
		// always disable foreign key check when do full sync.
		"foreign_key_checks": "0",
	}
	cfg.Mydumper.SourceID = subtaskCfg.SourceID
	return cfg, nil
}

func (l *LightningLoader) getLightningConfig() (*lcfg.Config, error) {
	cfg, err := GetLightningConfig(l.lightningGlobalConfig, l.cfg)
	if err != nil {
		return nil, err
	}
	cfg.TiDB.StrSQLMode = l.sqlMode
	cfg.TiDB.Vars["time_zone"] = l.timeZone
	return cfg, nil
}

func (l *LightningLoader) restore(ctx context.Context) error {
	if err := putLoadTask(l.cli, l.cfg, l.workerName); err != nil {
		return err
	}

	status, err := l.checkPointList.taskStatus(ctx)
	if err != nil {
		return err
	}

	// we have disabled auto-resume for below errors, so if lightning is resuming
	// it means user wants to skip this error.
	switch {
	case terror.ErrLoadLightningHasDup.Equal(l.lastErr),
		terror.ErrLoadLightningChecksum.Equal(l.lastErr):
		l.logger.Info("manually resume from error, DM will skip the error and continue to next unit",
			zap.Error(l.lastErr))

		l.finish.Store(true)
		err = l.checkPointList.UpdateStatus(ctx, lightningStatusFinished)
		if err != nil {
			l.logger.Error("failed to update checkpoint status", zap.Error(err))
			return err
		}
		status = lightningStatusFinished
	}

	if status < lightningStatusFinished {
		if err = l.checkPointList.RegisterCheckPoint(ctx); err != nil {
			return err
		}
		var cfg *lcfg.Config
		cfg, err = l.getLightningConfig()
		if err != nil {
			return err
		}
		if err2 := readyAndWait(ctx, l.cli, l.cfg); err2 != nil {
			return err2
		}
		err = l.runLightning(ctx, cfg)
		if err == nil {
			l.finish.Store(true)
			err = l.checkPointList.UpdateStatus(ctx, lightningStatusFinished)
			if err != nil {
				l.logger.Error("failed to update checkpoint status", zap.Error(err))
				return err
			}
		} else {
			l.logger.Error("failed to runlightning", zap.Error(err))
		}
	} else {
		l.finish.Store(true)
	}
	if err == nil && l.finish.Load() && l.cfg.Mode == config.ModeFull {
		if err = delLoadTask(l.cli, l.cfg, l.workerName); err != nil {
			return err
		}
	}
	if l.finish.Load() {
		if l.cfg.CleanDumpFile {
			cleanDumpFiles(ctx, l.cfg)
		}
		return finishAndWait(ctx, l.cli, l.cfg)
	}
	return err
}

func (l *LightningLoader) handleExitErrMetric(err *pb.ProcessError) {
	resumable := fmt.Sprintf("%t", unit.IsResumableError(err))
	l.metricProxies.loaderExitWithErrorCounter.WithLabelValues(l.cfg.Name, l.cfg.SourceID, resumable).Inc()
}

// Process implements Unit.Process.
func (l *LightningLoader) Process(ctx context.Context, pr chan pb.ProcessResult) {
	l.logger.Info("lightning load start")
	errs := make([]*pb.ProcessError, 0, 1)
	failpoint.Inject("lightningAlwaysErr", func(_ failpoint.Value) {
		l.logger.Info("", zap.String("failpoint", "lightningAlwaysErr"))
		pr <- pb.ProcessResult{
			Errors: []*pb.ProcessError{unit.NewProcessError(errors.New("failpoint lightningAlwaysErr"))},
		}
		failpoint.Return()
	})

	binlog, gtid, err := getMydumpMetadata(ctx, l.cli, l.cfg, l.workerName)
	if err != nil {
		processError := unit.NewProcessError(err)
		l.handleExitErrMetric(processError)
		pr <- pb.ProcessResult{
			Errors: []*pb.ProcessError{processError},
		}
		return
	}
	if binlog != "" {
		l.metaBinlog.Store(binlog)
	}
	if gtid != "" {
		l.metaBinlogGTID.Store(gtid)
	}

	failpoint.Inject("longLoadProcess", func(val failpoint.Value) {
		if sec, ok := val.(int); ok {
			l.logger.Info("long loader unit", zap.Int("second", sec))
			time.Sleep(time.Duration(sec) * time.Second)
		}
	})

	if err := l.restore(ctx); err != nil && !utils.IsContextCanceledError(err) {
		l.logger.Error("process error", zap.Error(err))
		processError := unit.NewProcessError(err)
		l.handleExitErrMetric(processError)
		errs = append(errs, processError)
	}
	isCanceled := false
	select {
	case <-ctx.Done():
		isCanceled = true
	default:
	}
	s := l.status()
	l.logger.Info("lightning load end",
		zap.Bool("IsCanceled", isCanceled),
		zap.Int64("finished_bytes", s.FinishedBytes),
		zap.Int64("total_bytes", s.TotalBytes),
		zap.String("progress", s.Progress))
	pr <- pb.ProcessResult{IsCanceled: isCanceled, Errors: errs}
}

func (l *LightningLoader) isClosed() bool {
	return l.closed.Load()
}

// IsFreshTask implements Unit.IsFreshTask.
func (l *LightningLoader) IsFreshTask(ctx context.Context) (bool, error) {
	status, err := l.checkPointList.taskStatus(ctx)
	return status == lightningStatusInit, err
}

// Close does graceful shutdown.
func (l *LightningLoader) Close() {
	l.Pause()
	l.removeLabelValuesWithTaskInMetrics(l.cfg.Name, l.cfg.SourceID)
	l.checkPointList.Close()
	l.closed.Store(true)
}

// Kill does ungraceful shutdown.
func (l *LightningLoader) Kill() {
	// TODO: implement kill
	l.Close()
}

// Pause pauses the process, and it can be resumed later
// should cancel context from external.
func (l *LightningLoader) Pause() {
	l.Lock()
	defer l.Unlock()
	if l.isClosed() {
		l.logger.Warn("try to pause, but already closed")
		return
	}
	if l.cancel != nil {
		l.cancel()
	}
	l.core.Stop()
}

// Resume resumes the paused process.
func (l *LightningLoader) Resume(ctx context.Context, pr chan pb.ProcessResult) {
	if l.isClosed() {
		l.logger.Warn("try to resume, but already closed")
		return
	}
	l.core = lserver.New(l.lightningGlobalConfig)
	// continue the processing
	l.Process(ctx, pr)
}

// Update implements Unit.Update
// now, only support to update config for routes, filters, column-mappings, block-allow-list
// now no config diff implemented, so simply re-init use new config
// no binlog filter for loader need to update.
func (l *LightningLoader) Update(ctx context.Context, cfg *config.SubTaskConfig) error {
	l.Lock()
	defer l.Unlock()
	l.cfg.BAList = cfg.BAList
	l.cfg.RouteRules = cfg.RouteRules
	l.cfg.ColumnMappingRules = cfg.ColumnMappingRules
	return nil
}

func (l *LightningLoader) status() *pb.LoadStatus {
	finished, total := l.core.Status()
	progress := percent(finished, total, l.finish.Load())
	currentSpeed := int64(l.speedRecorder.GetSpeed(float64(finished)))

	l.logger.Info("progress status of lightning",
		zap.Int64("finished_bytes", finished),
		zap.Int64("total_bytes", total),
		zap.String("progress", progress),
		zap.Int64("current speed (bytes / seconds)", currentSpeed),
	)
	s := &pb.LoadStatus{
		FinishedBytes:  finished,
		TotalBytes:     total,
		Progress:       progress,
		MetaBinlog:     l.metaBinlog.Load(),
		MetaBinlogGTID: l.metaBinlogGTID.Load(),
		Bps:            currentSpeed,
	}
	return s
}

// Status returns the unit's current status.
func (l *LightningLoader) Status(_ *binlog.SourceStatus) interface{} {
	return l.status()
}

func connParamFromConfig(config *lcfg.Config) *common.MySQLConnectParam {
	return &common.MySQLConnectParam{
		Host:     config.TiDB.Host,
		Port:     config.TiDB.Port,
		User:     config.TiDB.User,
		Password: config.TiDB.Psw,
		SQLMode:  mysql.DefaultSQLMode,
		// TODO: keep same as Lightning defaultMaxAllowedPacket later
		MaxAllowedPacket:         64 * 1024 * 1024,
		TLSConfig:                config.Security.TLSConfig,
		AllowFallbackToPlaintext: config.Security.AllowFallbackToPlaintext,
	}
}
