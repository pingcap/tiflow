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
	"sync"

	"github.com/docker/go-units"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/br/pkg/lightning"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	lcfg "github.com/pingcap/tidb/br/pkg/lightning/config"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/dm/unit"
	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/conn"
	tcontext "github.com/pingcap/dm/pkg/context"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
)

const (
	lightningCheckpointFile  = "lightning.checkpoint.0.sql"
	lightningCheckpointDB    = "lightning"
	lightningCheckpointTable = "checkpoint"
)

// LightningLoader can load your mydumper data into TiDB database.
type LightningLoader struct {
	sync.RWMutex

	cfg             *config.SubTaskConfig
	cli             *clientv3.Client
	checkPoint      CheckPoint
	workerName      string
	logger          log.Logger
	core            *lightning.Lightning
	toDB            *conn.BaseDB
	toDBConns       []*DBConn
	lightningConfig *lcfg.GlobalConfig

	finish         atomic.Bool
	closed         atomic.Bool
	metaBinlog     atomic.String
	metaBinlogGTID atomic.String
	cancel         context.CancelFunc // for per task context, which maybe different from lightning context
}

// NewLightning creates a new Loader importing data with lightning.
func NewLightning(cfg *config.SubTaskConfig, cli *clientv3.Client, workerName string) *LightningLoader {
	lightningCfg := makeGlobalConfig(cfg)
	core := lightning.New(lightningCfg)
	loader := &LightningLoader{
		cfg:             cfg,
		cli:             cli,
		core:            core,
		lightningConfig: lightningCfg,
		logger:          log.With(zap.String("task", cfg.Name), zap.String("unit", "lightning-load")),
		workerName:      workerName,
	}
	return loader
}

func makeGlobalConfig(cfg *config.SubTaskConfig) *lcfg.GlobalConfig {
	lightningCfg := lcfg.NewGlobalConfig()
	if cfg.To.Security != nil {
		lightningCfg.Security.CAPath = cfg.To.Security.SSLCA
		lightningCfg.Security.CertPath = cfg.To.Security.SSLCert
		lightningCfg.Security.KeyPath = cfg.To.Security.SSLKey
	}
	lightningCfg.TiDB.Host = cfg.To.Host
	lightningCfg.TiDB.Psw = cfg.To.Password
	lightningCfg.TiDB.User = cfg.To.User
	lightningCfg.TiDB.Port = cfg.To.Port
	lightningCfg.TiDB.StatusPort = cfg.TiDB.StatusPort
	lightningCfg.TiDB.PdAddr = cfg.TiDB.PdAddr
	lightningCfg.TikvImporter.Backend = cfg.TiDB.Backend
	lightningCfg.PostRestore.Checksum = lcfg.OpLevelOff
	if cfg.TiDB.Backend == lcfg.BackendLocal {
		lightningCfg.TikvImporter.SortedKVDir = cfg.Dir
	}
	lightningCfg.Mydumper.SourceDir = cfg.Dir
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
	checkpoint, err := newRemoteCheckPoint(tctx, l.cfg, l.checkpointID())
	failpoint.Inject("ignoreLoadCheckpointErr", func(_ failpoint.Value) {
		l.logger.Info("", zap.String("failpoint", "ignoreLoadCheckpointErr"))
		err = nil
	})
	l.checkPoint = checkpoint
	l.toDB, l.toDBConns, err = createConns(tctx, l.cfg, 1)
	return err
}

func (l *LightningLoader) restore(ctx context.Context) error {
	tctx := tcontext.NewContext(ctx, l.logger)
	if err := putLoadTask(l.cli, l.cfg, l.workerName); err != nil {
		return err
	}
	if err := l.checkPoint.Init(tctx, lightningCheckpointFile, 1); err != nil {
		return err
	}
	if err := l.checkPoint.Load(tctx); err != nil {
		return err
	}
	db2Tables := make(map[string]Tables2DataFiles)
	tables := make(Tables2DataFiles)
	files := make(DataFiles, 0, 1)
	files = append(files, lightningCheckpointFile)
	tables[lightningCheckpointTable] = files
	db2Tables[lightningCheckpointDB] = tables
	var err error
	if err = l.checkPoint.CalcProgress(db2Tables); err != nil {
		return err
	}
	if !l.checkPoint.IsTableFinished(lightningCheckpointDB, lightningCheckpointTable) {
		cfg := lcfg.NewConfig()
		if err = cfg.LoadFromGlobal(l.lightningConfig); err != nil {
			return err
		}
		cfg.Routes = l.cfg.RouteRules
		cfg.Checkpoint.Driver = lcfg.CheckpointDriverMySQL
		cfg.Checkpoint.Schema = config.TiDBLightningCheckpointPrefix + dbutil.ColumnName(l.workerName)
		param := common.MySQLConnectParam{
			Host:             cfg.TiDB.Host,
			Port:             cfg.TiDB.Port,
			User:             cfg.TiDB.User,
			Password:         cfg.TiDB.Psw,
			SQLMode:          mysql.DefaultSQLMode,
			MaxAllowedPacket: 64 * units.MiB,
			TLS:              cfg.TiDB.TLS,
		}
		cfg.Checkpoint.DSN = param.ToDSN()
		cfg.TiDB.StrSQLMode = l.cfg.LoaderConfig.SQLMode
		if err = cfg.Adjust(ctx); err != nil {
			return err
		}
		l.Lock()
		taskCtx, cancel := context.WithCancel(ctx)
		l.cancel = cancel
		l.Unlock()
		err = l.core.RunOnce(taskCtx, cfg, nil)
		if err == nil {
			l.finish.Store(true)
			offsetSQL := l.checkPoint.GenSQL(lightningCheckpointFile, 1)
			err = l.toDBConns[0].executeSQL(tctx, []string{offsetSQL})
			_ = l.checkPoint.UpdateOffset(lightningCheckpointFile, 1)
		}
	} else {
		l.finish.Store(true)
	}
	if l.cfg.Mode == config.ModeFull {
		if err = delLoadTask(l.cli, l.cfg, l.workerName); err != nil {
			return err
		}
	}
	if l.finish.Load() {
		if l.cfg.CleanDumpFile {
			cleanDumpFiles(l.cfg)
		}
	}
	return err
}

// Process implements Unit.Process.
func (l *LightningLoader) Process(ctx context.Context, pr chan pb.ProcessResult) {
	l.logger.Info("lightning load start")
	errs := make([]*pb.ProcessError, 0, 1)
	binlog, gtid, err := getMydumpMetadata(l.cli, l.cfg, l.workerName)
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
		errs = append(errs, unit.NewProcessError(err))
	}
	isCanceled := false
	select {
	case <-ctx.Done():
		isCanceled = true
	default:
	}
	l.logger.Info("lightning load end")
	pr <- pb.ProcessResult{
		IsCanceled: isCanceled,
		Errors:     errs,
	}
}

func (l *LightningLoader) isClosed() bool {
	return l.closed.Load()
}

// IsFreshTask implements Unit.IsFreshTask.
func (l *LightningLoader) IsFreshTask(ctx context.Context) (bool, error) {
	count, err := l.checkPoint.Count(tcontext.NewContext(ctx, l.logger))
	return count == 0, err
}

// Close does graceful shutdown.
func (l *LightningLoader) Close() {
	l.Pause()
	l.closed.Store(true)
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
	l.core = lightning.New(l.lightningConfig)
	// continue the processing
	l.Process(ctx, pr)
}

// Update implements Unit.Update
// now, only support to update config for routes, filters, column-mappings, block-allow-list
// now no config diff implemented, so simply re-init use new config
// no binlog filter for loader need to update.
func (l *LightningLoader) Update(cfg *config.SubTaskConfig) error {
	// update l.cfg
	l.cfg.BAList = cfg.BAList
	l.cfg.RouteRules = cfg.RouteRules
	l.cfg.ColumnMappingRules = cfg.ColumnMappingRules
	return nil
}

// Status returns the unit's current status.
func (l *LightningLoader) Status(_ *binlog.SourceStatus) interface{} {
	finished, total := l.core.Status()
	progress := percent(finished, total, l.finish.Load())
	s := &pb.LoadStatus{
		FinishedBytes:  finished,
		TotalBytes:     total,
		Progress:       progress,
		MetaBinlog:     l.metaBinlog.Load(),
		MetaBinlogGTID: l.metaBinlogGTID.Load(),
	}
	return s
}

// checkpointID returns ID which used for checkpoint table.
func (l *LightningLoader) checkpointID() string {
	if len(l.cfg.SourceID) > 0 {
		return l.cfg.SourceID
	}
	dir, err := filepath.Abs(l.cfg.Dir)
	if err != nil {
		l.logger.Warn("get abs dir", zap.String("directory", l.cfg.Dir), log.ShortError(err))
		return l.cfg.Dir
	}
	return shortSha1(dir)
}
