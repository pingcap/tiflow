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
	"path/filepath"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	lcfg "github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/dumpling/export"
	tidbpromutil "github.com/pingcap/tidb/util/promutil"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
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
	TmpTLSConfigPath            = "lightning_tls"
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
	core   *lightning.Lightning
	cancel context.CancelFunc // for per task context, which maybe different from lightning context

	toDBConns []*DBConn
	toDB      *conn.BaseDB

	workerName     string
	finish         atomic.Bool
	closed         atomic.Bool
	metaBinlog     atomic.String
	metaBinlogGTID atomic.String

	speedRecorder *export.SpeedRecorder
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
		core:                  lightning.New(lightningCfg),
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
	lightningCfg.TikvImporter.Backend = lcfg.BackendTiDB
	if cfg.LoaderConfig.ImportMode == config.LoadModePhysical {
		lightningCfg.TikvImporter.Backend = lcfg.BackendLocal
	}
	lightningCfg.PostRestore.Checksum = lcfg.OpLevelOff
	if lightningCfg.TikvImporter.Backend == lcfg.BackendLocal {
		lightningCfg.TikvImporter.SortedKVDir = cfg.Dir
	}
	lightningCfg.Mydumper.SourceDir = cfg.Dir
	lightningCfg.App.Config.File = "" // make lightning not init logger, see more in https://github.com/pingcap/tidb/pull/29291
	return lightningCfg
}

// Type implements Unit.Type.
func (l *LightningLoader) Type() pb.UnitType {
	return pb.UnitType_Load
}

// Init initializes loader for a load task, but not start Process.
// if fail, it should not call l.Close.
func (l *LightningLoader) Init(ctx context.Context) (err error) {
	tctx := tcontext.NewContext(ctx, l.logger)
	toCfg, err := l.cfg.Clone()
	if err != nil {
		return err
	}
	l.toDB, l.toDBConns, err = createConns(tctx, l.cfg, toCfg.Name, toCfg.SourceID, 1)
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
		baseDB, err2 := conn.DefaultDBProvider.Apply(&l.cfg.To)
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
		sqlModes, err3 := utils.AdjustSQLModeCompatible(l.cfg.LoaderConfig.SQLMode)
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
	var cpdb checkpoints.DB
	if l.cfg.ExtStorage != nil {
		cpdb, err = checkpoints.NewFileCheckpointsDBWithExstorageFileName(
			ctx, l.cfg.ExtStorage.URI(), l.cfg.ExtStorage, lightningCheckpointFileName)
	} else {
		cpdb, err = checkpoints.OpenCheckpointsDB(ctx, cfg)
	}
	if err != nil {
		return err
	}
	defer func() {
		_ = cpdb.Close()
	}()
	return errors.Trace(cpdb.IgnoreErrorCheckpoint(ctx, "all"))
}

func (l *LightningLoader) runLightning(ctx context.Context, cfg *lcfg.Config) error {
	taskCtx, cancel := context.WithCancel(ctx)
	l.Lock()
	l.cancel = cancel
	l.Unlock()

	// always try to skill all checkpoint errors so we can resume this phase.
	err := l.ignoreCheckpointError(ctx, cfg)
	if err != nil {
		l.logger.Warn("check lightning checkpoint status failed, skip this error", log.ShortError(err))
	}
	if err = l.checkPointList.UpdateStatus(ctx, lightningStatusRunning); err != nil {
		return err
	}

	var opts []lightning.Option
	if l.cfg.MetricsFactory != nil {
		// this branch means dataflow engine has set a Factory, the Factory itself
		// will register and deregister metrics, but lightning will expect the
		// register and deregister at the beginning and end of its lifetime.
		// So we use dataflow engine's Factory to register, and use dataflow engine's
		// global metrics to manually deregister.
		opts = append(opts,
			lightning.WithPromFactory(
				promutil.NewWrappingFactory(
					l.cfg.MetricsFactory,
					"",
					prometheus.Labels{"task": l.cfg.Name, "source_id": l.cfg.SourceID},
				)),
			lightning.WithPromRegistry(promutil.GetGlobalMetricRegistry()))
	} else {
		registry := prometheus.DefaultGatherer.(prometheus.Registerer)
		failpoint.Inject("DontUnregister", func() {
			registry = promutil.NewOnlyRegRegister(registry)
		})

		opts = append(opts,
			lightning.WithPromFactory(
				promutil.NewWrappingFactory(
					tidbpromutil.NewDefaultFactory(),
					"",
					prometheus.Labels{"task": l.cfg.Name, "source_id": l.cfg.SourceID},
				),
			),
			lightning.WithPromRegistry(registry))
	}
	if l.cfg.ExtStorage != nil {
		opts = append(opts,
			lightning.WithDumpFileStorage(l.cfg.ExtStorage),
			lightning.WithCheckpointStorage(l.cfg.ExtStorage, lightningCheckpointFileName))
	}
	if l.cfg.FrameworkLogger != nil {
		opts = append(opts, lightning.WithLogger(l.cfg.FrameworkLogger))
	} else {
		opts = append(opts, lightning.WithLogger(l.logger.Logger))
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
	return terror.ErrLoadLightningRuntime.Delegate(err)
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

	cfg.Checkpoint.Driver = lcfg.CheckpointDriverFile
	var cpPath string
	// l.cfg.LoaderConfig.Dir may be a s3 path, and Lightning supports checkpoint in s3, we can use storage.AdjustPath to adjust path both local and s3.
	cpPath, err := storage.AdjustPath(subtaskCfg.LoaderConfig.Dir, string(filepath.Separator)+lightningCheckpointFileName)
	if err != nil {
		return nil, err
	}
	cfg.Checkpoint.DSN = cpPath
	cfg.Checkpoint.KeepAfterSuccess = lcfg.CheckpointOrigin

	cfg.TikvImporter.OnDuplicate = string(subtaskCfg.OnDuplicateLogical)
	cfg.TiDB.Vars = make(map[string]string)
	cfg.Routes = subtaskCfg.RouteRules
	if subtaskCfg.To.Session != nil {
		for k, v := range subtaskCfg.To.Session {
			cfg.TiDB.Vars[k] = v
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

	if status < lightningStatusFinished {
		if err = l.checkPointList.RegisterCheckPoint(ctx); err != nil {
			return err
		}
		var cfg *lcfg.Config
		cfg, err = l.getLightningConfig()
		if err != nil {
			return err
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
	}
	return err
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
		loaderExitWithErrorCounter.WithLabelValues(l.cfg.Name, l.cfg.SourceID).Inc()
		pr <- pb.ProcessResult{
			Errors: []*pb.ProcessError{unit.NewProcessError(err)},
		}
		return
	}
	if binlog != "" {
		l.metaBinlog.Store(binlog)
	}
	if gtid != "" {
		l.metaBinlogGTID.Store(gtid)
	}

	if err := l.restore(ctx); err != nil && !utils.IsContextCanceledError(err) {
		l.logger.Error("process error", zap.Error(err))
		errs = append(errs, unit.NewProcessError(err))
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
	l.core = lightning.New(l.lightningGlobalConfig)
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
