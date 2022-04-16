// Copyright 2019 PingCAP, Inc.
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
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/errgroup"

	regexprrouter "github.com/pingcap/tidb/util/regexpr-router"
	router "github.com/pingcap/tidb/util/table-router"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/dm/unit"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	fr "github.com/pingcap/tiflow/dm/pkg/func-rollback"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	cm "github.com/pingcap/tidb-tools/pkg/column-mapping"
	"github.com/pingcap/tidb/util/filter"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	jobCount            = 1000
	uninitializedOffset = -1
)

// FilePosSet represents a set in mathematics.
type FilePosSet map[string][]int64

// DataFiles represent all data files for a single table.
type DataFiles []string

// Tables2DataFiles represent all data files of a table collection as a map.
type Tables2DataFiles map[string]DataFiles

type dataJob struct {
	sql          string
	schema       string
	table        string
	sourceTable  string
	sourceSchema string
	file         string
	absPath      string
	offset       int64
	lastOffset   int64
}

type fileJob struct {
	schema   string
	table    string
	dataFile string
	offset   int64
	info     *tableInfo
}

// Worker represents a worker.
type Worker struct {
	id         int
	cfg        *config.SubTaskConfig
	checkPoint CheckPoint
	conn       *DBConn
	wg         sync.WaitGroup
	jobQueue   chan *dataJob
	loader     *Loader

	logger log.Logger

	closed atomic.Bool
}

// NewWorker returns a Worker.
func NewWorker(loader *Loader, id int) *Worker {
	w := &Worker{
		id:         id,
		cfg:        loader.cfg,
		checkPoint: loader.checkPoint,
		conn:       loader.toDBConns[id],
		jobQueue:   make(chan *dataJob, jobCount),
		loader:     loader,
		logger:     loader.logger.WithFields(zap.Int("worker ID", id)),
	}

	failpoint.Inject("workerChanSize", func(val failpoint.Value) {
		size := val.(int)
		w.logger.Info("", zap.String("failpoint", "workerChanSize"), zap.Int("size", size))
		w.jobQueue = make(chan *dataJob, size)
	})

	return w
}

// Close closes worker.
func (w *Worker) Close() {
	// simulate the case that doesn't wait all doJob goroutine exit
	failpoint.Inject("workerCantClose", func(_ failpoint.Value) {
		w.logger.Info("", zap.String("failpoint", "workerCantClose"))
		failpoint.Return()
	})

	if !w.closed.CAS(false, true) {
		w.wg.Wait()
		w.logger.Info("already closed...")
		return
	}

	w.logger.Info("start to close...")
	close(w.jobQueue)
	w.wg.Wait()
	w.logger.Info("closed !!!")
}

func (w *Worker) run(ctx context.Context, fileJobQueue chan *fileJob, runFatalChan chan *pb.ProcessError) {
	w.closed.Store(false)

	newCtx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		// make sure all doJob goroutines exit
		w.Close()
	}()

	ctctx := tcontext.NewContext(newCtx, w.logger)

	doJob := func() {
		hasError := false
		for {
			job, ok := <-w.jobQueue
			if !ok {
				w.logger.Info("job queue was closed, execution goroutine exits")
				return
			}
			if job == nil {
				w.logger.Info("jobs are finished, execution goroutine exits")
				return
			}
			if hasError {
				continue // continue to read so than the sender will not be blocked
			}

			sqls := make([]string, 0, 3)
			sqls = append(sqls, "USE `"+unescapePercent(job.schema, w.logger)+"`;")
			sqls = append(sqls, job.sql)

			offsetSQL := w.checkPoint.GenSQL(job.file, job.offset)
			sqls = append(sqls, offsetSQL)

			failpoint.Inject("LoadExceedOffsetExit", func(val failpoint.Value) {
				threshold, _ := val.(int)
				if job.offset >= int64(threshold) {
					w.logger.Warn("load offset execeeds threshold, it will exit", zap.Int64("load offset", job.offset), zap.Int("value", threshold), zap.String("failpoint", "LoadExceedOffsetExit"))
					utils.OsExit(1)
				}
			})

			failpoint.Inject("LoadDataSlowDown", nil)

			failpoint.Inject("LoadDataSlowDownByTask", func(val failpoint.Value) {
				tasks := val.(string)
				taskNames := strings.Split(tasks, ",")
				for _, taskName := range taskNames {
					if w.cfg.Name == taskName {
						w.logger.Info("inject failpoint LoadDataSlowDownByTask", zap.String("task", taskName))
						<-newCtx.Done()
					}
				}
			})

			startTime := time.Now()
			err := w.conn.executeSQL(ctctx, sqls)
			failpoint.Inject("executeSQLError", func(_ failpoint.Value) {
				w.logger.Info("", zap.String("failpoint", "executeSQLError"))
				err = errors.New("inject failpoint executeSQLError")
			})
			if err != nil {
				// expect pause rather than exit
				err = terror.WithScope(terror.Annotatef(err, "file %s", job.file), terror.ScopeDownstream)
				if !utils.IsContextCanceledError(err) {
					runFatalChan <- unit.NewProcessError(err)
				}
				hasError = true
				failpoint.Inject("returnDoJobError", func(_ failpoint.Value) {
					w.logger.Info("", zap.String("failpoint", "returnDoJobError"))
					failpoint.Return()
				})
				continue
			}
			txnHistogram.WithLabelValues(w.cfg.Name, w.cfg.WorkerName, w.cfg.SourceID, job.schema, job.table).Observe(time.Since(startTime).Seconds())
			failpoint.Inject("loaderCPUpdateOffsetError", func(_ failpoint.Value) {
				job.file = "notafile" + job.file
			})
			if err := w.loader.checkPoint.UpdateOffset(job.file, job.offset); err != nil {
				runFatalChan <- unit.NewProcessError(err)
				hasError = true
				continue
			}
			// update finished offset after checkpoint updated
			w.loader.finishedDataSize.Add(job.offset - job.lastOffset)
			if _, ok := w.loader.dbTableDataFinishedSize[job.sourceSchema]; ok {
				if _, ok := w.loader.dbTableDataFinishedSize[job.sourceSchema][job.sourceTable]; ok {
					w.loader.dbTableDataFinishedSize[job.sourceSchema][job.sourceTable].Store(job.offset)
				}
			}
		}
	}

	// worker main routine
	for {
		select {
		case <-newCtx.Done():
			w.logger.Info("context canceled, main goroutine exits")
			return
		case job, ok := <-fileJobQueue:
			if !ok {
				w.logger.Info("file queue was closed, main routine exit.")
				return
			}

			w.wg.Add(1)
			go func() {
				defer w.wg.Done()
				doJob()
			}()

			// restore a table
			if err := w.restoreDataFile(ctx, filepath.Join(w.cfg.Dir, job.dataFile), job.offset, job.info); err != nil {
				// expect pause rather than exit
				err = terror.Annotatef(err, "restore data file (%v) failed", job.dataFile)
				if !utils.IsContextCanceledError(err) {
					runFatalChan <- unit.NewProcessError(err)
				}
				return
			}
		}
	}
}

func (w *Worker) restoreDataFile(ctx context.Context, filePath string, offset int64, table *tableInfo) error {
	w.logger.Info("start to restore dump sql file", zap.String("data file", filePath))
	err := w.dispatchSQL(ctx, filePath, offset, table)
	if err != nil {
		return err
	}

	failpoint.Inject("dispatchError", func(_ failpoint.Value) {
		w.logger.Info("", zap.String("failpoint", "dispatchError"))
		failpoint.Return(errors.New("inject failpoint dispatchError"))
	})

	// dispatchSQL completed, send nil to make sure all dmls are applied to target database
	// we don't want to close and re-make chan frequently
	// but if we need to re-call w.run, we need re-make jobQueue chan
	w.jobQueue <- nil
	w.wg.Wait()

	w.logger.Info("finish to restore dump sql file", zap.String("data file", filePath))
	return nil
}

func (w *Worker) dispatchSQL(ctx context.Context, file string, offset int64, table *tableInfo) error {
	var (
		f   *os.File
		err error
		cur int64
	)

	baseFile := filepath.Base(file)

	f, err = os.Open(file)
	if err != nil {
		return terror.ErrLoadUnitDispatchSQLFromFile.Delegate(err)
	}
	defer f.Close()

	// file was not found in checkpoint
	if offset == uninitializedOffset {
		offset = 0

		finfo, err2 := f.Stat()
		if err2 != nil {
			return terror.ErrLoadUnitDispatchSQLFromFile.Delegate(err2)
		}

		tctx := tcontext.NewContext(ctx, w.logger)
		err2 = w.checkPoint.Init(tctx, baseFile, finfo.Size())
		failpoint.Inject("WaitLoaderStopAfterInitCheckpoint", func(v failpoint.Value) {
			t := v.(int)
			w.logger.Info("wait loader stop after init checkpoint")
			w.wg.Add(1)
			time.Sleep(time.Duration(t) * time.Second)
			w.wg.Done()
		})

		if err2 != nil {
			w.logger.Error("fail to initial checkpoint", zap.String("data file", file), zap.Int64("offset", offset), log.ShortError(err2))
			return err2
		}
	}

	cur, err = f.Seek(offset, io.SeekStart)
	if err != nil {
		return terror.ErrLoadUnitDispatchSQLFromFile.Delegate(err)
	}
	w.logger.Debug("read file", zap.String("data file", file), zap.Int64("offset", offset))

	lastOffset := cur

	data := make([]byte, 0, 1024*1024)
	br := bufio.NewReader(f)
	for {
		select {
		case <-ctx.Done():
			w.logger.Info("sql dispatcher is ready to quit.", zap.String("data file", file), zap.Int64("offset", offset))
			return nil
		default:
			// do nothing
		}
		line, err := br.ReadString('\n')
		if err == io.EOF {
			w.logger.Info("data are scanned finished.", zap.String("data file", file), zap.Int64("offset", offset))
			break
		}
		cur += int64(len(line))

		realLine := strings.TrimSpace(line[:len(line)-1])
		if len(realLine) == 0 {
			continue
		}

		data = append(data, []byte(line)...)
		if realLine[len(realLine)-1] == ';' {
			query := strings.TrimSpace(string(data))
			if strings.HasPrefix(query, "/*") && strings.HasSuffix(query, "*/;") {
				data = data[0:0]
				continue
			}

			// extend column also need use reassemble to write SQL and the table name has been renamed
			if w.loader.columnMapping != nil || len(table.extendCol) > 0 {
				// column mapping and route table
				query, err = reassemble(data, table, w.loader.columnMapping)
				if err != nil {
					return terror.Annotatef(err, "file %s", file)
				}
			} else if table.sourceTable != table.targetTable {
				// dumped data files always use backquote as quotes
				query = renameShardingTable(query, table.sourceTable, table.targetTable, false)
			}

			idx := strings.Index(query, "INSERT INTO")
			if idx < 0 {
				return terror.ErrLoadUnitInvalidInsertSQL.Generate(query)
			}

			data = data[0:0]

			j := &dataJob{
				sql:          query,
				schema:       table.targetSchema,
				table:        table.targetTable,
				sourceSchema: table.sourceSchema,
				sourceTable:  table.sourceTable,
				file:         baseFile,
				absPath:      file,
				offset:       cur,
				lastOffset:   lastOffset,
			}
			lastOffset = cur

			w.jobQueue <- j
		}
	}

	return nil
}

type tableInfo struct {
	sourceSchema   string
	sourceTable    string
	targetSchema   string
	targetTable    string
	columnNameList []string
	insertHeadStmt string
	extendCol      []string
	extendVal      []string
}

// Loader can load your mydumper data into TiDB database.
type Loader struct {
	sync.RWMutex

	cfg        *config.SubTaskConfig
	cli        *clientv3.Client
	workerName string
	checkPoint CheckPoint

	logger log.Logger

	// db -> tables
	// table -> data files
	db2Tables  map[string]Tables2DataFiles
	tableInfos map[string]*tableInfo

	fileJobQueue chan *fileJob

	tableRouter   *regexprrouter.RouteTable
	baList        *filter.Filter
	columnMapping *cm.Mapping

	toDB      *conn.BaseDB
	toDBConns []*DBConn

	totalFileCount   atomic.Int64 // schema + table + data
	totalDataSize    atomic.Int64
	finishedDataSize atomic.Int64

	// to calculate remainingTimeGauge metric, map will be init in `l.prepare.prepareDataFiles`
	dbTableDataTotalSize        map[string]map[string]*atomic.Int64
	dbTableDataFinishedSize     map[string]map[string]*atomic.Int64
	dbTableDataLastFinishedSize map[string]map[string]*atomic.Int64
	dbTableDataLastUpdatedTime  atomic.Time

	metaBinlog     atomic.String
	metaBinlogGTID atomic.String

	// record process error rather than log.Fatal
	runFatalChan chan *pb.ProcessError

	// for every worker goroutine, not for every data file
	workerWg *sync.WaitGroup
	// for other goroutines
	wg sync.WaitGroup

	fileJobQueueClosed atomic.Bool
	finish             atomic.Bool
	closed             atomic.Bool
}

// NewLoader creates a new Loader.
func NewLoader(cfg *config.SubTaskConfig, cli *clientv3.Client, workerName string) *Loader {
	loader := &Loader{
		cfg:        cfg,
		cli:        cli,
		db2Tables:  make(map[string]Tables2DataFiles),
		tableInfos: make(map[string]*tableInfo),
		workerWg:   new(sync.WaitGroup),
		logger:     log.With(zap.String("task", cfg.Name), zap.String("unit", "load")),
		workerName: workerName,
	}
	loader.fileJobQueueClosed.Store(true) // not open yet
	return loader
}

// Type implements Unit.Type.
func (l *Loader) Type() pb.UnitType {
	return pb.UnitType_Load
}

// Init initializes loader for a load task, but not start Process.
// if fail, it should not call l.Close.
func (l *Loader) Init(ctx context.Context) (err error) {
	rollbackHolder := fr.NewRollbackHolder("loader")
	defer func() {
		if err != nil {
			rollbackHolder.RollbackReverseOrder()
		}
	}()

	tctx := tcontext.NewContext(ctx, l.logger)

	checkpoint, err := newRemoteCheckPoint(tctx, l.cfg, l.checkpointID())
	failpoint.Inject("ignoreLoadCheckpointErr", func(_ failpoint.Value) {
		l.logger.Info("", zap.String("failpoint", "ignoreLoadCheckpointErr"))
		err = nil
	})
	if err != nil {
		return err
	}
	l.checkPoint = checkpoint
	rollbackHolder.Add(fr.FuncRollback{Name: "close-checkpoint", Fn: l.checkPoint.Close})

	l.baList, err = filter.New(l.cfg.CaseSensitive, l.cfg.BAList)
	if err != nil {
		return terror.ErrLoadUnitGenBAList.Delegate(err)
	}

	err = l.genRouter(l.cfg.RouteRules)
	if err != nil {
		return err
	}

	if len(l.cfg.ColumnMappingRules) > 0 {
		l.columnMapping, err = cm.NewMapping(l.cfg.CaseSensitive, l.cfg.ColumnMappingRules)
		if err != nil {
			return terror.ErrLoadUnitGenColumnMapping.Delegate(err)
		}
	}

	dbCfg := l.cfg.To
	dbCfg.RawDBCfg = config.DefaultRawDBConfig().
		SetMaxIdleConns(l.cfg.PoolSize)

	// used to change loader's specified DB settings, currently SQL Mode
	lcfg, err := l.cfg.Clone()
	if err != nil {
		return err
	}
	// fix nil map after clone, which we will use below
	// TODO: we may develop `SafeClone` in future
	if lcfg.To.Session == nil {
		lcfg.To.Session = make(map[string]string)
	}
	timeZone := l.cfg.Timezone
	if len(timeZone) == 0 {
		var err1 error
		timeZone, err1 = conn.FetchTimeZoneSetting(ctx, &lcfg.To)
		if err1 != nil {
			return err1
		}
	}
	lcfg.To.Session["time_zone"] = timeZone

	hasSQLMode := false
	for k := range l.cfg.To.Session {
		if strings.ToLower(k) == "sql_mode" {
			hasSQLMode = true
			break
		}
	}

	if !hasSQLMode {
		sqlModes, err3 := utils.AdjustSQLModeCompatible(l.cfg.LoaderConfig.SQLMode)
		if err3 != nil {
			l.logger.Warn("cannot adjust sql_mode compatible, the sql_mode will stay the same", log.ShortError(err3))
		}
		lcfg.To.Session["sql_mode"] = sqlModes
	}

	l.logger.Info("loader's sql_mode is", zap.String("sqlmode", lcfg.To.Session["sql_mode"]))

	l.toDB, l.toDBConns, err = createConns(tctx, lcfg, lcfg.Name, lcfg.SourceID, l.cfg.PoolSize)
	if err != nil {
		return err
	}

	return nil
}

// Process implements Unit.Process.
func (l *Loader) Process(ctx context.Context, pr chan pb.ProcessResult) {
	loaderExitWithErrorCounter.WithLabelValues(l.cfg.Name, l.cfg.SourceID).Add(0)

	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	l.newFileJobQueue()
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

	l.runFatalChan = make(chan *pb.ProcessError, 2*l.cfg.PoolSize)
	errs := make([]*pb.ProcessError, 0, 2)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range l.runFatalChan {
			cancel() // cancel l.Restore
			errs = append(errs, err)
		}
	}()

	err = l.Restore(newCtx)
	close(l.runFatalChan) // Restore returned, all potential fatal sent to l.runFatalChan
	cancel()              // cancel the goroutines created in `Restore`.

	failpoint.Inject("dontWaitWorkerExit", func(_ failpoint.Value) {
		l.logger.Info("", zap.String("failpoint", "dontWaitWorkerExit"))
		l.workerWg.Wait()
	})

	wg.Wait() // wait for receive all fatal from l.runFatalChan

	if err != nil {
		if utils.IsContextCanceledError(err) {
			l.logger.Info("filter out error caused by user cancel")
		} else {
			loaderExitWithErrorCounter.WithLabelValues(l.cfg.Name, l.cfg.SourceID).Inc()
			errs = append(errs, unit.NewProcessError(err))
		}
	}

	isCanceled := false
	select {
	case <-ctx.Done():
		isCanceled = true
	default:
	}

	pr <- pb.ProcessResult{
		IsCanceled: isCanceled,
		Errors:     errs,
	}
}

func (l *Loader) newFileJobQueue() {
	l.closeFileJobQueue()
	l.fileJobQueue = make(chan *fileJob, jobCount)
	l.fileJobQueueClosed.Store(false)
}

func (l *Loader) closeFileJobQueue() {
	if l.fileJobQueueClosed.Load() {
		return
	}
	close(l.fileJobQueue)
	l.fileJobQueueClosed.Store(true)
}

// align with https://github.com/pingcap/dumpling/pull/140
// if input is malformed, return original string and print log.
func unescapePercent(input string, logger log.Logger) string {
	buf := bytes.Buffer{}
	buf.Grow(len(input))
	i := 0
	for i < len(input) {
		if input[i] != '%' {
			buf.WriteByte(input[i])
			i++
		} else {
			if i+2 >= len(input) {
				logger.Error("malformed filename while unescapePercent", zap.String("filename", input))
				return input
			}
			ascii, err := hex.DecodeString(input[i+1 : i+3])
			if err != nil {
				logger.Error("malformed filename while unescapePercent", zap.String("filename", input))
				return input
			}
			buf.Write(ascii)
			i += 3
		}
	}
	return buf.String()
}

func (l *Loader) skipSchemaAndTable(table *filter.Table) bool {
	if filter.IsSystemSchema(table.Schema) {
		return true
	}

	table.Schema = unescapePercent(table.Schema, l.logger)
	table.Name = unescapePercent(table.Name, l.logger)

	tbs := []*filter.Table{table}
	tbs = l.baList.Apply(tbs)
	return len(tbs) == 0
}

func (l *Loader) isClosed() bool {
	return l.closed.Load()
}

// IsFreshTask implements Unit.IsFreshTask.
func (l *Loader) IsFreshTask(ctx context.Context) (bool, error) {
	count, err := l.checkPoint.Count(tcontext.NewContext(ctx, l.logger))
	return count == 0, err
}

// Restore begins the restore process.
func (l *Loader) Restore(ctx context.Context) error {
	if err := putLoadTask(l.cli, l.cfg, l.workerName); err != nil {
		return err
	}

	if err := l.prepare(); err != nil {
		l.logger.Error("scan directory failed", zap.String("directory", l.cfg.Dir), log.ShortError(err))
		return err
	}

	failpoint.Inject("WaitLoaderStopBeforeLoadCheckpoint", func(v failpoint.Value) {
		t := v.(int)
		l.logger.Info("wait loader stop before load checkpoint")
		l.wg.Add(1)
		time.Sleep(time.Duration(t) * time.Second)
		l.wg.Done()
	})

	// not update checkpoint in memory when restoring, so when re-Restore, we need to load checkpoint from DB
	err := l.checkPoint.Load(tcontext.NewContext(ctx, l.logger))
	if err != nil {
		return err
	}
	err = l.checkPoint.CalcProgress(l.db2Tables)
	if err != nil {
		l.logger.Error("calc load process", log.ShortError(err))
		return err
	}
	l.loadFinishedSize()
	if err2 := l.initAndStartWorkerPool(ctx); err2 != nil {
		l.logger.Error("initial and start worker pools failed", log.ShortError(err))
		return err2
	}

	begin := time.Now()
	err = l.restoreData(ctx)

	failpoint.Inject("dontWaitWorkerExit", func(_ failpoint.Value) {
		l.logger.Info("", zap.String("failpoint", "dontWaitWorkerExit"))
		failpoint.Return(nil)
	})

	// make sure all workers exit
	l.closeFileJobQueue() // all data file dispatched, close it
	l.workerWg.Wait()

	if err == nil {
		l.finish.Store(true)
		l.logger.Info("all data files have been finished", zap.Duration("cost time", time.Since(begin)))
		if l.checkPoint.AllFinished() {
			if l.cfg.Mode == config.ModeFull {
				if err = delLoadTask(l.cli, l.cfg, l.workerName); err != nil {
					return err
				}
			}
			if l.cfg.CleanDumpFile {
				cleanDumpFiles(ctx, l.cfg)
			}
		}
	} else if errors.Cause(err) != context.Canceled {
		return err
	}

	return nil
}

func (l *Loader) loadFinishedSize() {
	results := l.checkPoint.GetAllRestoringFileInfo()
	for file, pos := range results {
		db, table, err := getDBAndTableFromFilename(file)
		if err != nil {
			l.logger.Warn("invalid db table sql file", zap.String("file", file), zap.Error(err))
			continue
		}
		l.finishedDataSize.Add(pos[0])
		l.dbTableDataFinishedSize[db][table].Add(pos[0])
	}
}

// Close does graceful shutdown.
func (l *Loader) Close() {
	l.Lock()
	defer l.Unlock()
	if l.isClosed() {
		return
	}

	l.stopLoad()

	if err := l.toDB.Close(); err != nil {
		l.logger.Error("close downstream DB error", log.ShortError(err))
	}
	l.checkPoint.Close()
	l.removeLabelValuesWithTaskInMetrics(l.cfg.Name)
	l.closed.Store(true)
}

// Kill kill the loader without graceful.
func (l *Loader) Kill() {
	// TODO: implement kill
	l.Close()
}

// stopLoad stops loading, now it used by Close and Pause
// maybe we can refine the workflow more clear.
func (l *Loader) stopLoad() {
	// before re-write workflow, simply close all job queue and job workers
	// when resuming, re-create them
	l.logger.Info("stop importing data process")

	l.closeFileJobQueue()
	l.workerWg.Wait()
	l.logger.Debug("all workers have been closed")

	l.wg.Wait()
	l.logger.Debug("all loader's go-routines have been closed")
}

// Pause implements Unit.Pause.
func (l *Loader) Pause() {
	if l.isClosed() {
		l.logger.Warn("try to pause, but already closed")
		return
	}

	l.stopLoad()
}

// Resume resumes the paused process.
func (l *Loader) Resume(ctx context.Context, pr chan pb.ProcessResult) {
	if l.isClosed() {
		l.logger.Warn("try to resume, but already closed")
		return
	}

	if err := l.resetDBs(ctx); err != nil {
		pr <- pb.ProcessResult{
			IsCanceled: false,
			Errors: []*pb.ProcessError{
				unit.NewProcessError(err),
			},
		}
		return
	}
	// continue the processing
	l.Process(ctx, pr)
}

func (l *Loader) resetDBs(ctx context.Context) error {
	var err error
	tctx := tcontext.NewContext(ctx, l.logger)

	for i := 0; i < len(l.toDBConns); i++ {
		err = l.toDBConns[i].resetConn(tctx)
		if err != nil {
			return terror.WithScope(err, terror.ScopeDownstream)
		}
	}

	err = l.checkPoint.ResetConn(tctx)
	if err != nil {
		return terror.WithScope(err, terror.ScopeDownstream)
	}

	return nil
}

// Update implements Unit.Update
// now, only support to update config for routes, filters, column-mappings, block-allow-list
// now no config diff implemented, so simply re-init use new config
// no binlog filter for loader need to update.
func (l *Loader) Update(ctx context.Context, cfg *config.SubTaskConfig) error {
	var (
		err              error
		oldBaList        *filter.Filter
		oldTableRouter   *regexprrouter.RouteTable
		oldColumnMapping *cm.Mapping
	)

	defer func() {
		if err == nil {
			return
		}
		if oldBaList != nil {
			l.baList = oldBaList
		}
		if oldTableRouter != nil {
			l.tableRouter = oldTableRouter
		}
		if oldColumnMapping != nil {
			l.columnMapping = oldColumnMapping
		}
	}()

	// update block-allow-list
	oldBaList = l.baList
	l.baList, err = filter.New(cfg.CaseSensitive, cfg.BAList)
	if err != nil {
		return terror.ErrLoadUnitGenBAList.Delegate(err)
	}

	// update route, for loader, this almost useless, because schemas often have been restored
	oldTableRouter = l.tableRouter
	l.tableRouter, err = regexprrouter.NewRegExprRouter(cfg.CaseSensitive, cfg.RouteRules)
	if err != nil {
		return terror.ErrLoadUnitGenTableRouter.Delegate(err)
	}

	// update column-mappings
	oldColumnMapping = l.columnMapping
	l.columnMapping, err = cm.NewMapping(cfg.CaseSensitive, cfg.ColumnMappingRules)
	if err != nil {
		return terror.ErrLoadUnitGenColumnMapping.Delegate(err)
	}

	// update l.cfg
	l.cfg.BAList = cfg.BAList
	l.cfg.RouteRules = cfg.RouteRules
	l.cfg.ColumnMappingRules = cfg.ColumnMappingRules
	return nil
}

func (l *Loader) genRouter(rules []*router.TableRule) error {
	l.tableRouter, _ = regexprrouter.NewRegExprRouter(l.cfg.CaseSensitive, []*router.TableRule{})
	for _, rule := range rules {
		err := l.tableRouter.AddRule(rule)
		if err != nil {
			return terror.ErrLoadUnitGenTableRouter.Delegate(err)
		}
	}
	schemaRules, tableRules := l.tableRouter.AllRules()
	l.logger.Debug("all route rules", zap.Reflect("schema route rules", schemaRules), zap.Reflect("table route rules", tableRules))
	return nil
}

func (l *Loader) initAndStartWorkerPool(ctx context.Context) error {
	for i := 0; i < l.cfg.PoolSize; i++ {
		worker := NewWorker(l, i)
		l.workerWg.Add(1) // for every worker goroutine, Add(1)
		go func() {
			defer l.workerWg.Done()
			worker.run(ctx, l.fileJobQueue, l.runFatalChan)
		}()
	}
	return nil
}

func (l *Loader) prepareDBFiles(files map[string]struct{}) error {
	// reset some variables
	l.db2Tables = make(map[string]Tables2DataFiles)
	l.totalFileCount.Store(0) // reset
	schemaFileCount := 0
	for file := range files {
		db, ok := utils.GetDBFromDumpFilename(file)
		if !ok {
			continue
		}
		schemaFileCount++
		if l.skipSchemaAndTable(&filter.Table{Schema: db}) {
			l.logger.Warn("ignore schema file", zap.String("schema file", file))
			continue
		}

		l.db2Tables[db] = make(Tables2DataFiles)
		l.totalFileCount.Add(1) // for schema
	}

	if schemaFileCount == 0 {
		l.logger.Warn("invalid mydumper files for there are no `-schema-create.sql` files found, and will generate later")
	}
	if len(l.db2Tables) == 0 {
		l.logger.Warn("no available `-schema-create.sql` files, check mydumper parameter matches block-allow-list in task config, will generate later")
	}

	return nil
}

func (l *Loader) prepareTableFiles(files map[string]struct{}) error {
	var tablesNumber float64
	for file := range files {
		db, table, ok := utils.GetTableFromDumpFilename(file)
		if !ok {
			continue
		}
		if l.skipSchemaAndTable(&filter.Table{Schema: db, Name: table}) {
			l.logger.Warn("ignore table file", zap.String("table file", file))
			continue
		}
		tables, ok := l.db2Tables[db]
		if !ok {
			l.logger.Warn("can't find schema create file, will generate one", zap.String("schema", db))
			if err := generateSchemaCreateFile(l.cfg.Dir, db); err != nil {
				return err
			}
			l.db2Tables[db] = make(Tables2DataFiles)
			tables = l.db2Tables[db]
			l.totalFileCount.Add(1)
		}

		if _, ok := tables[table]; ok {
			return terror.ErrLoadUnitDuplicateTableFile.Generate(file)
		}
		tablesNumber++
		tables[table] = make(DataFiles, 0, 16)
		l.totalFileCount.Add(1) // for table
	}

	tableGauge.WithLabelValues(l.cfg.Name, l.cfg.SourceID).Set(tablesNumber)
	return nil
}

func (l *Loader) prepareDataFiles(files map[string]struct{}) error {
	var dataFilesNumber float64

	for file := range files {
		if !strings.HasSuffix(file, ".sql") || strings.Contains(file, "-schema.sql") ||
			strings.Contains(file, "-schema-create.sql") {
			continue
		}

		// ignore view / triggers
		if strings.Contains(file, "-schema-view.sql") || strings.Contains(file, "-schema-triggers.sql") ||
			strings.Contains(file, "-schema-post.sql") {
			l.logger.Warn("ignore unsupport view/trigger file", zap.String("file", file))
			continue
		}

		db, table, err := getDBAndTableFromFilename(file)
		if err != nil {
			l.logger.Warn("invalid db table sql file", zap.String("file", file), zap.Error(err))
			continue
		}
		if l.skipSchemaAndTable(&filter.Table{Schema: db, Name: table}) {
			l.logger.Warn("ignore data file", zap.String("data file", file))
			continue
		}
		tables, ok := l.db2Tables[db]
		if !ok {
			return terror.ErrLoadUnitNoDBFile.Generate(file)
		}

		dataFiles, ok := tables[table]
		if !ok {
			return terror.ErrLoadUnitNoTableFile.Generate(file)
		}

		size, err := utils.GetFileSize(filepath.Join(l.cfg.Dir, file))
		if err != nil {
			return err
		}
		l.totalDataSize.Add(size)
		l.totalFileCount.Add(1) // for data
		if _, ok := l.dbTableDataTotalSize[db]; !ok {
			l.dbTableDataTotalSize[db] = make(map[string]*atomic.Int64)
			l.dbTableDataFinishedSize[db] = make(map[string]*atomic.Int64)
			l.dbTableDataLastFinishedSize[db] = make(map[string]*atomic.Int64)
		}
		if _, ok := l.dbTableDataTotalSize[db][table]; !ok {
			l.dbTableDataTotalSize[db][table] = atomic.NewInt64(0)
			l.dbTableDataFinishedSize[db][table] = atomic.NewInt64(0)
			l.dbTableDataLastFinishedSize[db][table] = atomic.NewInt64(0)
		}
		l.dbTableDataTotalSize[db][table].Add(size)

		dataFiles = append(dataFiles, file)
		dataFilesNumber++
		tables[table] = dataFiles
	}

	dataFileGauge.WithLabelValues(l.cfg.Name, l.cfg.SourceID).Set(dataFilesNumber)
	dataSizeGauge.WithLabelValues(l.cfg.Name, l.cfg.SourceID).Set(float64(l.totalDataSize.Load()))
	return nil
}

func (l *Loader) prepare() error {
	begin := time.Now()
	defer func() {
		l.logger.Info("prepare loading", zap.Duration("cost time", time.Since(begin)))
	}()

	// reset some counter used to calculate progress
	l.totalDataSize.Store(0)
	l.finishedDataSize.Store(0) // reset before load from checkpoint
	l.dbTableDataTotalSize = make(map[string]map[string]*atomic.Int64)
	l.dbTableDataFinishedSize = make(map[string]map[string]*atomic.Int64)
	l.dbTableDataLastFinishedSize = make(map[string]map[string]*atomic.Int64)

	// check if mydumper dir data exists.
	if !utils.IsDirExists(l.cfg.Dir) {
		// compatibility with no `.name` suffix
		dirSuffix := "." + l.cfg.Name
		var trimmed bool
		if strings.HasSuffix(l.cfg.Dir, dirSuffix) {
			dirPrefix := strings.TrimSuffix(l.cfg.Dir, dirSuffix)
			if utils.IsDirExists(dirPrefix) {
				l.logger.Warn("directory doesn't exist, try to load data from old fashion directory", zap.String("directory", l.cfg.Dir), zap.String("old fashion directory", dirPrefix))
				l.cfg.Dir = dirPrefix
				trimmed = true
			}
		}
		if !trimmed {
			return terror.ErrLoadUnitDumpDirNotFound.Generate(l.cfg.Dir)
		}
	}

	// collect dir files.
	files, err := utils.CollectDirFiles(l.cfg.Dir)
	if err != nil {
		return err
	}

	l.logger.Debug("collected files", zap.Reflect("files", files))

	/* Mydumper file names format
	 * db    {db}-schema-create.sql
	 * table {db}.{table}-schema.sql
	 * sql   {db}.{table}.{part}.sql or {db}.{table}.sql
	 */

	// Sql file for create db
	if err := l.prepareDBFiles(files); err != nil {
		return err
	}

	// Sql file for create table
	if err := l.prepareTableFiles(files); err != nil {
		return err
	}

	// Sql file for restore data
	return l.prepareDataFiles(files)
}

// restoreSchema creates schema.
func (l *Loader) restoreSchema(ctx context.Context, conn *DBConn, sqlFile, schema string) error {
	if l.checkPoint.IsTableCreated(schema, "") {
		l.logger.Info("database already exists in checkpoint, skip creating it", zap.String("schema", schema), zap.String("db schema file", sqlFile))
		return nil
	}
	err := l.restoreStructure(ctx, conn, sqlFile, schema, "")
	if err != nil {
		if isErrDBExists(err) {
			l.logger.Info("database already exists, skip it", zap.String("db schema file", sqlFile))
		} else {
			return terror.Annotatef(err, "run db schema failed - dbfile %s", sqlFile)
		}
	}
	return nil
}

// restoreTable creates table.
func (l *Loader) restoreTable(ctx context.Context, conn *DBConn, sqlFile, schema, table string) error {
	if l.checkPoint.IsTableCreated(schema, table) {
		l.logger.Info("table already exists in checkpoint, skip creating it", zap.String("schema", schema), zap.String("table", table), zap.String("db schema file", sqlFile))
		return nil
	}
	err := l.restoreStructure(ctx, conn, sqlFile, schema, table)
	if err != nil {
		if isErrTableExists(err) {
			l.logger.Info("table already exists, skip it", zap.String("table schema file", sqlFile))
		} else {
			return terror.Annotatef(err, "run table schema failed - dbfile %s", sqlFile)
		}
	}
	return nil
}

// restoreStruture creates schema or table.
func (l *Loader) restoreStructure(ctx context.Context, conn *DBConn, sqlFile string, schema string, table string) error {
	f, err := os.Open(sqlFile)
	if err != nil {
		return terror.ErrLoadUnitReadSchemaFile.Delegate(err)
	}
	defer f.Close()

	tctx := tcontext.NewContext(ctx, l.logger)
	ansiquote := strings.Contains(l.cfg.SQLMode, "ANSI_QUOTES")

	data := make([]byte, 0, 1024*1024)
	br := bufio.NewReader(f)
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		}

		realLine := strings.TrimSpace(line[:len(line)-1])
		if len(realLine) == 0 {
			continue
		}

		data = append(data, []byte(realLine)...)
		if data[len(data)-1] == ';' {
			query := string(data)
			data = data[0:0]
			if strings.HasPrefix(query, "/*") && strings.HasSuffix(query, "*/;") {
				continue
			}

			var sqls []string
			dstSchema, dstTable := fetchMatchedLiteral(tctx, l.tableRouter, schema, table)
			// for table
			if table != "" {
				sqls = append(sqls, "USE `"+unescapePercent(dstSchema, l.logger)+"`;")
				query = renameShardingTable(query, table, dstTable, ansiquote)
			} else {
				query = renameShardingSchema(query, schema, dstSchema, ansiquote)
			}

			l.logger.Debug("schema create statement", zap.String("sql", query))

			sqls = append(sqls, query)
			err = conn.executeSQL(tctx, sqls)
			if err != nil {
				return terror.WithScope(err, terror.ScopeDownstream)
			}
		}
	}

	return nil
}

// renameShardingTable replaces srcTable with dstTable in query.
func renameShardingTable(query, srcTable, dstTable string, ansiquote bool) string {
	return SQLReplace(query, srcTable, dstTable, ansiquote)
}

// renameShardingSchema replaces srcSchema with dstSchema in query.
func renameShardingSchema(query, srcSchema, dstSchema string, ansiquote bool) string {
	return SQLReplace(query, srcSchema, dstSchema, ansiquote)
}

func fetchMatchedLiteral(ctx *tcontext.Context, router *regexprrouter.RouteTable, schema, table string) (targetSchema string, targetTable string) {
	if schema == "" {
		// nothing change
		return schema, table
	}

	targetSchema, targetTable, err := router.Route(schema, table)
	if err != nil {
		ctx.L().Error("fail to route table", zap.Error(err)) // log the error, but still continue
	}
	if targetSchema == "" {
		// nothing change
		return schema, table
	}
	if targetTable == "" {
		// table still same;
		targetTable = table
	}

	return targetSchema, targetTable
}

// `restore Schema Job` present a data structure of schema restoring job.
type restoreSchemaJob struct {
	loader   *Loader
	session  *DBConn
	database string // database name
	table    string // table name, empty if it's a schema of database
	filepath string // file path of dumpped schema file
}

// `jobQueue` of schema restoring which (only) support consumptions concurrently.
type jobQueue struct {
	ctx           context.Context
	msgq          chan *restoreSchemaJob // job message queue channel
	consumerCount int                    // count of consumers
	eg            *errgroup.Group        // err wait group of consumer's go-routines
}

// `newJobQueue` consturct a jobQueue.
func newJobQueue(ctx context.Context, consumerCount, length int) *jobQueue {
	eg, selfCtx := errgroup.WithContext(ctx)
	return &jobQueue{
		ctx:           selfCtx,
		msgq:          make(chan *restoreSchemaJob, length),
		consumerCount: consumerCount,
		eg:            eg,
	}
}

// `push` will append a job to the queue.
func (q *jobQueue) push(job *restoreSchemaJob) error {
	var err error
	select {
	case <-q.ctx.Done():
		err = q.ctx.Err()
	case q.msgq <- job:
	}
	return terror.WithScope(err, terror.ScopeInternal)
}

// `close` wait jobs done and close queue forever.
func (q *jobQueue) close() error {
	// queue is closing
	close(q.msgq)
	// wait until go-routines of consumption was exited
	return q.eg.Wait()
}

// `startConsumers` run multiple go-routines of job consumption with user defined handler.
func (q *jobQueue) startConsumers(handler func(ctx context.Context, job *restoreSchemaJob) error) {
	for i := 0; i < q.consumerCount; i++ {
		q.eg.Go(func() error {
			var session *DBConn
		consumeLoop:
			for {
				select {
				case <-q.ctx.Done():
					err := q.ctx.Err()
					return err
				case job, active := <-q.msgq:
					if !active {
						break consumeLoop
					}
					// test condition for `job.session` means db session still could be controlled outside,
					// it's used in unit test for now.
					if session == nil && job.session == nil {
						baseConn, err2 := job.loader.toDB.GetBaseConn(q.ctx)
						if err2 != nil {
							return err2
						}
						defer func(baseConn *conn.BaseConn) {
							err2 := job.loader.toDB.CloseBaseConn(baseConn)
							if err2 != nil {
								job.loader.logger.Warn("fail to close connection", zap.Error(err2))
							}
						}(baseConn)
						session = &DBConn{
							name:     job.loader.cfg.Name,
							sourceID: job.loader.cfg.SourceID,
							baseConn: baseConn,
							resetBaseConnFn: func(*tcontext.Context, *conn.BaseConn) (*conn.BaseConn, error) {
								return nil, terror.ErrDBBadConn.Generate("bad connection error restoreData")
							},
						}
					}
					if job.session == nil {
						job.session = session
					}
					err := handler(q.ctx, job)
					if err != nil {
						return err
					}
				}
			}
			return nil
		})
	}
}

func (l *Loader) restoreData(ctx context.Context) error {
	begin := time.Now()
	dispatchMap := make(map[string]*fileJob)
	concurrency := l.cfg.PoolSize
	// `for v := range map` would present random order
	// `dbs` array keep same order for restore schema job generating
	var err error
	dbs := make([]string, 0, len(l.db2Tables))
	for db := range l.db2Tables {
		dbs = append(dbs, db)
	}
	tctx := tcontext.NewContext(ctx, l.logger)

	// run consumers of restore database schema queue
	dbRestoreQueue := newJobQueue(ctx, concurrency, concurrency /** length of queue */)
	dbRestoreQueue.startConsumers(func(ctx context.Context, job *restoreSchemaJob) error {
		// restore database schema
		job.loader.logger.Info("start to create schema", zap.String("schema file", job.filepath))
		err2 := job.loader.restoreSchema(ctx, job.session, job.filepath, job.database)
		if err2 != nil {
			return err2
		}
		job.loader.logger.Info("finish to create schema", zap.String("schema file", job.filepath))
		return nil
	})

	// push database schema restoring jobs to the queue
	for _, db := range dbs {
		schemaFile := l.cfg.Dir + "/" + db + "-schema-create.sql" // cache friendly
		err = dbRestoreQueue.push(&restoreSchemaJob{
			loader:   l,
			database: db,
			table:    "",
			filepath: schemaFile,
		})
		if err != nil {
			break
		}
	}

	// check producing error
	if err != nil {
		runtimeErr := dbRestoreQueue.close()
		if errors.ErrorEqual(err, context.Canceled) {
			err = runtimeErr
		}
		return err
	}
	// wait whole task done & close queue
	err = dbRestoreQueue.close()
	if err != nil {
		return err
	}

	// run consumers of restore table schema queue
	tblRestoreQueue := newJobQueue(ctx, concurrency, concurrency /** length of queue */)
	tblRestoreQueue.startConsumers(func(ctx context.Context, job *restoreSchemaJob) error {
		job.loader.logger.Info("start to create table", zap.String("table file", job.filepath))
		err2 := job.loader.restoreTable(ctx, job.session, job.filepath, job.database, job.table)
		if err2 != nil {
			return err2
		}
		job.loader.logger.Info("finish to create table", zap.String("table file", job.filepath))
		return nil
	})

	// push table schema restoring jobs to the queue
tblSchemaLoop:
	for _, db := range dbs {
		for table := range l.db2Tables[db] {
			schemaFile := l.cfg.Dir + "/" + db + "." + table + "-schema.sql" // cache friendly
			if _, ok := l.tableInfos[tableName(db, table)]; !ok {
				l.tableInfos[tableName(db, table)], err = parseTable(tctx, l.tableRouter, db, table, schemaFile, l.cfg.LoaderConfig.SQLMode, l.cfg.SourceID)
				if err != nil {
					err = terror.Annotatef(err, "parse table %s/%s", db, table)
					break tblSchemaLoop
				}
			}
			if l.checkPoint.IsTableFinished(db, table) {
				l.logger.Info("table has finished, skip it.", zap.String("schema", db), zap.String("table", table))
				continue
			}
			err = tblRestoreQueue.push(&restoreSchemaJob{
				loader:   l,
				database: db,
				table:    table,
				filepath: schemaFile,
			})
			if err != nil {
				break tblSchemaLoop
			}
		}
	}

	// check producing error
	if err != nil {
		runtimeErr := tblRestoreQueue.close()
		if errors.ErrorEqual(err, context.Canceled) {
			err = runtimeErr
		}
		return err
	}
	// wait whole task done & close queue
	err = tblRestoreQueue.close()
	if err != nil {
		return err
	}

	// all schemas was restored
	l.logger.Info("finish to create tables", zap.Duration("cost time", time.Since(begin)))

	// generate restore table data file job
	for _, db := range dbs {
		table2DataFileMap := l.db2Tables[db]
		for table := range table2DataFileMap {
			restoringFiles := l.checkPoint.GetRestoringFileInfo(db, table)
			l.logger.Debug("restoring table data", zap.String("schema", db), zap.String("table", table), zap.Reflect("data files", restoringFiles))

			for _, file := range table2DataFileMap[table] {
				select {
				case <-ctx.Done():
					l.logger.Warn("stop generate data file job", log.ShortError(ctx.Err()))
					return ctx.Err()
				default:
					// do nothing
				}
				l.logger.Debug("dispatch data file", zap.String("schema", db), zap.String("table", table), zap.String("data file", file))

				offset := int64(uninitializedOffset)
				posSet, ok := restoringFiles[file]
				if ok {
					offset = posSet[0]
				}
				dispatchMap[db+"_"+table+"_"+file] = &fileJob{
					schema:   db,
					table:    table,
					dataFile: file,
					offset:   offset,
					info:     l.tableInfos[tableName(db, table)],
				}
			}
		}
	}

	// a simple and naive approach to dispatch files randomly based on the feature of golang map(range by random)
	for _, j := range dispatchMap {
		select {
		case <-ctx.Done():
			l.logger.Warn("stop dispatch data file job", log.ShortError(ctx.Err()))
			return ctx.Err()
		case l.fileJobQueue <- j:
		}
	}

	l.logger.Info("all data files have been dispatched, waiting for them finished")
	return nil
}

// checkpointID returns ID which used for checkpoint table.
func (l *Loader) checkpointID() string {
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
