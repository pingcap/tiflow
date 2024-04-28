// Copyright 2020 PingCAP, Inc.
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

package dumpling

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/dumpling/export"
	tidbpromutil "github.com/pingcap/tidb/pkg/util/promutil"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	dutils "github.com/pingcap/tiflow/dm/pkg/dumpling"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/storage"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/unit"
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Dumpling dumps full data from a MySQL-compatible database.
type Dumpling struct {
	cfg           *config.SubTaskConfig
	metricProxies *metricProxies

	logger log.Logger

	dumpConfig *export.Config
	closed     atomic.Bool
	core       *export.Dumper
	mu         sync.RWMutex
}

// NewDumpling creates a new Dumpling.
func NewDumpling(cfg *config.SubTaskConfig) *Dumpling {
	logger := log.L()
	if cfg.FrameworkLogger != nil {
		logger = log.Logger{Logger: cfg.FrameworkLogger}
	}
	m := &Dumpling{
		cfg:    cfg,
		logger: logger.WithFields(zap.String("task", cfg.Name), zap.String("unit", "dump")),
	}
	return m
}

// Init implements Unit.Init.
func (m *Dumpling) Init(ctx context.Context) error {
	var err error
	if m.dumpConfig, err = m.constructArgs(ctx); err != nil {
		return err
	}
	if m.cfg.MetricsFactory != nil {
		// this branch means dataflow engine has set a Factory, the Factory itself
		// will register and deregister metrics, so we must use NoopRegistry
		// to avoid duplicated registration.
		m.metricProxies = &metricProxies{}
		m.metricProxies.dumplingExitWithErrorCounter = m.cfg.MetricsFactory.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "dm",
				Subsystem: "dumpling",
				Name:      "exit_with_error_count",
				Help:      "counter for dumpling exit with error",
			}, []string{"task", "source_id", "resumable_err"},
		)
		m.dumpConfig.PromFactory = promutil.NewWrappingFactory(
			m.cfg.MetricsFactory,
			"",
			prometheus.Labels{
				"task": m.cfg.Name, "source_id": m.cfg.SourceID,
			},
		)
		m.dumpConfig.PromRegistry = tidbpromutil.NewNoopRegistry()
	} else {
		m.metricProxies = defaultMetricProxies
		m.dumpConfig.PromFactory = promutil.NewWrappingFactory(
			promutil.NewPromFactory(),
			"",
			prometheus.Labels{
				"task": m.cfg.Name, "source_id": m.cfg.SourceID,
			},
		)
		m.dumpConfig.PromRegistry = prometheus.DefaultGatherer.(prometheus.Registerer)
	}

	m.logger.Info("create dumpling", zap.Stringer("config", m.dumpConfig))
	return nil
}

func (m *Dumpling) handleExitErrMetric(err *pb.ProcessError) {
	resumable := fmt.Sprintf("%t", unit.IsResumableError(err))
	m.metricProxies.dumplingExitWithErrorCounter.WithLabelValues(m.cfg.Name, m.cfg.SourceID, resumable).Inc()
}

// Process implements Unit.Process.
func (m *Dumpling) Process(ctx context.Context, pr chan pb.ProcessResult) {
	m.metricProxies.dumplingExitWithErrorCounter.WithLabelValues(m.cfg.Name, m.cfg.SourceID, "true").Add(0)
	m.metricProxies.dumplingExitWithErrorCounter.WithLabelValues(m.cfg.Name, m.cfg.SourceID, "false").Add(0)

	failpoint.Inject("dumpUnitProcessWithError", func(val failpoint.Value) {
		m.logger.Info("dump unit runs with injected error", zap.String("failpoint", "dumpUnitProcessWithError"), zap.Reflect("error", val))
		msg, ok := val.(string)
		if !ok {
			msg = "unknown process error"
		}
		pr <- pb.ProcessResult{
			IsCanceled: false,
			Errors:     []*pb.ProcessError{unit.NewProcessError(errors.New(msg))},
		}
		failpoint.Return()
	})

	begin := time.Now()
	errs := make([]*pb.ProcessError, 0, 1)

	failpoint.Inject("dumpUnitProcessForever", func() {
		m.logger.Info("dump unit runs forever", zap.String("failpoint", "dumpUnitProcessForever"))
		<-ctx.Done()
		pr <- pb.ProcessResult{
			IsCanceled: true,
			Errors:     []*pb.ProcessError{unit.NewProcessError(ctx.Err())},
		}
		failpoint.Return()
	})

	// NOTE: remove output dir before start dumping
	// every time re-dump, loader should re-prepare
	// when engine has opened an ExternalStorage, we can assume it's empty.
	if m.cfg.ExtStorage == nil {
		err := storage.RemoveAll(ctx, m.cfg.Dir, nil)
		if err != nil {
			m.logger.Error("fail to remove output directory", zap.String("directory", m.cfg.Dir), log.ShortError(err))
			processError := unit.NewProcessError(terror.ErrDumpUnitRuntime.Delegate(err, "fail to remove output directory: "+m.cfg.Dir))
			m.handleExitErrMetric(processError)
			errs = append(errs, processError)
			pr <- pb.ProcessResult{
				IsCanceled: false,
				Errors:     errs,
			}
			return
		}
	}

	failpoint.Inject("dumpUnitProcessCancel", func() {
		m.logger.Info("mock dump unit cancel", zap.String("failpoint", "dumpUnitProcessCancel"))
		<-ctx.Done()
	})

	newCtx, cancel := context.WithCancel(ctx)
	var (
		dumpling *export.Dumper
		err      error
	)
	if dumpling, err = export.NewDumper(newCtx, m.dumpConfig); err == nil {
		m.mu.Lock()
		m.core = dumpling
		m.mu.Unlock()
		err = dumpling.Dump()
		failpoint.Inject("SleepBeforeDumplingClose", func(val failpoint.Value) {
			t := val.(int)
			time.Sleep(time.Second * time.Duration(t))
			m.logger.Info("", zap.String("failpoint", "SleepBeforeDumplingClose"))
		})
		dumpling.Close()
	} else {
		m.logger.Warn("error occurred during NewDumper", zap.Error(err))
	}
	cancel()

	if err != nil {
		if utils.IsContextCanceledError(err) {
			m.logger.Info("filter out error caused by user cancel")
		} else {
			processError := unit.NewProcessError(terror.ErrDumpUnitRuntime.Delegate(err, ""))
			m.handleExitErrMetric(processError)
			errs = append(errs, processError)
		}
	}

	isCanceled := false
	select {
	case <-ctx.Done():
		isCanceled = true
	default:
	}

	if len(errs) == 0 {
		m.logger.Info("dump data finished", zap.Duration("cost time", time.Since(begin)))
	} else {
		m.logger.Error("dump data exits with error", zap.Duration("cost time", time.Since(begin)),
			zap.String("error", unit.JoinProcessErrors(errs)))
	}

	failpoint.Inject("dumpUnitProcessNoError", func() {
		m.logger.Info("dump unit runs no error", zap.String("failpoint", "dumpUnitProcessNoError"))
		errs = errs[:0]
	})

	pr <- pb.ProcessResult{
		IsCanceled: isCanceled,
		Errors:     errs,
	}
}

// Close implements Unit.Close.
func (m *Dumpling) Close() {
	if m.closed.Load() {
		return
	}

	m.removeLabelValuesWithTaskInMetrics(m.cfg.Name, m.cfg.SourceID)
	// do nothing, external will cancel the command (if running)
	m.closed.Store(true)
}

// Kill implements Unit.Kill.
func (m *Dumpling) Kill() {
	// TODO: implement kill
	m.Close()
}

// Pause implements Unit.Pause.
func (m *Dumpling) Pause() {
	if m.closed.Load() {
		m.logger.Warn("try to pause, but already closed")
		return
	}
	// do nothing, external will cancel the command (if running)
}

// Resume implements Unit.Resume.
func (m *Dumpling) Resume(ctx context.Context, pr chan pb.ProcessResult) {
	if m.closed.Load() {
		m.logger.Warn("try to resume, but already closed")
		return
	}
	// just call Process
	m.Process(ctx, pr)
}

// Update implements Unit.Update.
func (m *Dumpling) Update(context.Context, *config.SubTaskConfig) error {
	// not support update configuration now
	return nil
}

// Status implements Unit.Status.
func (m *Dumpling) Status(_ *binlog.SourceStatus) interface{} {
	// NOTE: try to add some status, like dumped file count
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.core == nil {
		return &pb.DumpStatus{}
	}
	return m.status()
}

func (m *Dumpling) status() *pb.DumpStatus {
	dumpStatus := m.core.GetStatus()
	s := &pb.DumpStatus{
		TotalTables:       dumpStatus.TotalTables,
		CompletedTables:   dumpStatus.CompletedTables,
		FinishedBytes:     dumpStatus.FinishedBytes,
		FinishedRows:      dumpStatus.FinishedRows,
		EstimateTotalRows: dumpStatus.EstimateTotalRows,
		Progress:          dumpStatus.Progress,
		Bps:               int64(dumpStatus.CurrentSpeedBPS),
	}
	var estimateProgress string
	if s.FinishedRows >= s.EstimateTotalRows {
		estimateProgress = "100.00%"
	} else {
		estimateProgress = fmt.Sprintf("%.2f %%", s.FinishedRows/s.EstimateTotalRows*100)
	}
	m.logger.Info("progress status of dumpling",
		zap.Int64("total_tables", s.TotalTables),
		zap.Int64("finished_tables", int64(s.CompletedTables)),
		zap.Int64("estimated_total_rows", int64(s.EstimateTotalRows)),
		zap.Int64("finished_rows", int64(s.FinishedRows)),
		zap.String("estimated_progress", estimateProgress),
		zap.String("new progress", s.Progress),
		zap.Int64("bps", s.Bps),
	)
	return s
}

// Type implements Unit.Type.
func (m *Dumpling) Type() pb.UnitType {
	return pb.UnitType_Dump
}

// IsFreshTask implements Unit.IsFreshTask.
func (m *Dumpling) IsFreshTask(ctx context.Context) (bool, error) {
	return true, nil
}

// constructArgs constructs arguments for exec.Command.
func (m *Dumpling) constructArgs(ctx context.Context) (*export.Config, error) {
	cfg := m.cfg
	db := cfg.From

	dumpConfig := export.DefaultConfig()

	// block status addr because we already have it in DM, and if we enable it, may we need more ports for the process.
	dumpConfig.StatusAddr = ""

	dumpConfig.Host = db.Host
	dumpConfig.Port = db.Port
	dumpConfig.User = db.User
	dumpConfig.Password = db.Password
	dumpConfig.OutputDirPath = cfg.Dir // use LoaderConfig.Dir as output dir
	dumpConfig.CollationCompatible = cfg.CollationCompatible
	tableFilter, err := filter.ParseMySQLReplicationRules(cfg.BAList)
	if err != nil {
		return nil, err
	}
	dumpConfig.TableFilter = tableFilter
	dumpConfig.CompleteInsert = true // always keep column name in `INSERT INTO` statements.
	dumpConfig.Logger = m.logger.Logger

	tz := m.cfg.Timezone
	if len(tz) == 0 {
		// use target db time_zone as default
		baseDB, err2 := conn.GetDownstreamDB(&m.cfg.To)
		if err2 != nil {
			return nil, err2
		}
		defer baseDB.Close()
		var err1 error
		tz, err1 = config.FetchTimeZoneSetting(ctx, baseDB.DB)
		if err1 != nil {
			return nil, err1
		}
	}
	dumpConfig.SessionParams = map[string]interface{}{
		"time_zone": tz,
	}

	if cfg.Threads > 0 {
		dumpConfig.Threads = cfg.Threads
	}
	if cfg.ChunkFilesize != "" {
		dumpConfig.FileSize, err = utils.ParseFileSize(cfg.ChunkFilesize, export.UnspecifiedSize)
		if err != nil {
			m.logger.Warn("parsed some unsupported arguments", zap.Error(err))
			return nil, err
		}
	}
	if cfg.StatementSize > 0 {
		dumpConfig.StatementSize = cfg.StatementSize
	}
	if cfg.Rows > 0 {
		dumpConfig.Rows = cfg.Rows
	}
	if len(cfg.Where) > 0 {
		dumpConfig.Where = cfg.Where
	}

	if db.Security != nil {
		dumpConfig.Security.CAPath = db.Security.SSLCA
		dumpConfig.Security.CertPath = db.Security.SSLCert
		dumpConfig.Security.KeyPath = db.Security.SSLKey

		dumpConfig.Security.SSLCABytes = db.Security.SSLCABytes
		dumpConfig.Security.SSLCertBytes = db.Security.SSLCertBytes
		dumpConfig.Security.SSLKeyBytes = db.Security.SSLKeyBytes
	}

	// `true` means dumpling will release lock after working connection established
	dumpConfig.TransactionalConsistency = true

	extraArgs := strings.Fields(cfg.ExtraArgs)
	if len(extraArgs) > 0 {
		err := dutils.ParseExtraArgs(&m.logger, dumpConfig, dutils.ParseArgLikeBash(extraArgs))
		if err != nil {
			m.logger.Warn("parsed some unsupported arguments", zap.Error(err))
		}
	}

	// record exit position when consistency is none, to support scenarios like Aurora upstream
	if dumpConfig.Consistency == "none" {
		dumpConfig.PosAfterConnect = true
	}
	// set default Rows
	if dumpConfig.Rows == export.UnspecifiedSize {
		dumpConfig.Rows = 200000
	}

	if !cfg.CaseSensitive {
		dumpConfig.TableFilter = filter.CaseInsensitive(dumpConfig.TableFilter)
	}

	// update sql_mode if needed
	m.detectSQLMode(ctx, dumpConfig)
	dumpConfig.ExtStorage = cfg.ExtStorage
	dumpConfig.MinTLSVersion = tls.VersionTLS10

	return dumpConfig, nil
}

// detectSQLMode tries to detect SQL mode from upstream. If success, write it to LoaderConfig.
// Because loader will use this SQL mode, we need to treat disable `EscapeBackslash` when NO_BACKSLASH_ESCAPES.
func (m *Dumpling) detectSQLMode(ctx context.Context, dumpCfg *export.Config) {
	baseDB, err := conn.GetUpstreamDB(&m.cfg.From)
	if err != nil {
		log.L().Warn("set up db connect failed", zap.Any("db", m.cfg.From),
			zap.Error(err))
		return
	}
	defer baseDB.Close()

	sqlMode, err := conn.GetGlobalVariable(tcontext.NewContext(ctx, log.L()), baseDB, "sql_mode")
	if err != nil {
		log.L().Warn("get global sql_mode from upstream failed", zap.Any("db", m.cfg.From), zap.Error(err))
		return
	}
	m.logger.Info("found upstream SQL mode", zap.String("SQL mode", sqlMode))
	m.cfg.LoaderConfig.SQLMode = sqlMode
	if strings.Contains(sqlMode, "NO_BACKSLASH_ESCAPES") {
		dumpCfg.EscapeBackslash = false
	} else {
		dumpCfg.EscapeBackslash = true
	}
}
