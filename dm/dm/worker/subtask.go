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

package worker

import (
	"context"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/failpoint"
	"github.com/prometheus/client_golang/prometheus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/dm/unit"
	"github.com/pingcap/tiflow/dm/dumpling"
	"github.com/pingcap/tiflow/dm/loader"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/shardddl/pessimism"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/relay"
	"github.com/pingcap/tiflow/dm/syncer"
)

const (
	// the timeout to wait for relay catchup when switching from load unit to sync unit.
	waitRelayCatchupTimeout = 30 * time.Second
)

// createRealUnits is subtask units initializer
// it can be used for testing.
var createUnits = createRealUnits

// createRealUnits creates process units base on task mode.
func createRealUnits(cfg *config.SubTaskConfig, etcdClient *clientv3.Client, workerName string, relay relay.Process) []unit.Unit {
	failpoint.Inject("mockCreateUnitsDumpOnly", func(_ failpoint.Value) {
		log.L().Info("create mock worker units with dump unit only", zap.String("failpoint", "mockCreateUnitsDumpOnly"))
		failpoint.Return([]unit.Unit{dumpling.NewDumpling(cfg)})
	})

	us := make([]unit.Unit, 0, 3)
	switch cfg.Mode {
	case config.ModeAll:
		us = append(us, dumpling.NewDumpling(cfg))
		us = append(us, newLoadUnit(cfg, etcdClient, workerName))
		us = append(us, syncer.NewSyncer(cfg, etcdClient, relay))
	case config.ModeFull:
		// NOTE: maybe need another checker in the future?
		us = append(us, dumpling.NewDumpling(cfg))
		us = append(us, newLoadUnit(cfg, etcdClient, workerName))
	case config.ModeIncrement:
		us = append(us, syncer.NewSyncer(cfg, etcdClient, relay))
	default:
		log.L().Error("unsupported task mode", zap.String("subtask", cfg.Name), zap.String("task mode", cfg.Mode))
	}
	return us
}

func newLoadUnit(cfg *config.SubTaskConfig, etcdClient *clientv3.Client, workerName string) unit.Unit {
	hasAutoGenColumn := false
	for _, rule := range cfg.RouteRules {
		if rule.SchemaExtractor != nil || rule.TableExtractor != nil || rule.SourceExtractor != nil {
			hasAutoGenColumn = true
			break
		}
	}
	// tidb-lightning doesn't support column mapping currently
	if cfg.ImportMode == config.LoadModeLoader || cfg.OnDuplicate == config.OnDuplicateError || hasAutoGenColumn || len(cfg.ColumnMappingRules) > 0 {
		return loader.NewLoader(cfg, etcdClient, workerName)
	}
	return loader.NewLightning(cfg, etcdClient, workerName)
}

// SubTask represents a sub task of data migration.
type SubTask struct {
	cfg *config.SubTaskConfig

	initialized atomic.Bool

	l log.Logger

	sync.RWMutex
	// ctx is used for the whole subtask. It will be created only when we new a subtask.
	ctx    context.Context
	cancel context.CancelFunc
	// currCtx is used for one loop. It will be created each time we use st.run/st.Resume
	currCtx    context.Context
	currCancel context.CancelFunc

	units    []unit.Unit // units do job one by one
	currUnit unit.Unit
	prevUnit unit.Unit
	resultWg sync.WaitGroup

	stage  pb.Stage          // stage of current sub task
	result *pb.ProcessResult // the process result, nil when is processing

	etcdClient *clientv3.Client

	workerName string

	validator *syncer.DataValidator
}

// NewSubTask is subtask initializer
// it can be used for testing.
var NewSubTask = NewRealSubTask

// NewRealSubTask creates a new SubTask.
func NewRealSubTask(cfg *config.SubTaskConfig, etcdClient *clientv3.Client, workerName string) *SubTask {
	return NewSubTaskWithStage(cfg, pb.Stage_New, etcdClient, workerName)
}

// NewSubTaskWithStage creates a new SubTask with stage.
func NewSubTaskWithStage(cfg *config.SubTaskConfig, stage pb.Stage, etcdClient *clientv3.Client, workerName string) *SubTask {
	ctx, cancel := context.WithCancel(context.Background())
	st := SubTask{
		cfg:        cfg,
		stage:      stage,
		l:          log.With(zap.String("subtask", cfg.Name)),
		ctx:        ctx,
		cancel:     cancel,
		etcdClient: etcdClient,
		workerName: workerName,
	}
	updateTaskMetric(st.cfg.Name, st.cfg.SourceID, st.stage, st.workerName)
	return &st
}

// initUnits initializes the sub task processing units.
func (st *SubTask) initUnits(relay relay.Process) error {
	// NOTE: because lightning not support init tls with raw certs bytes, we write the certs data to a file.
	if st.cfg.NeedUseLightning() && st.cfg.To.Security != nil {
		// NOTE: LoaderConfig.Dir may be a s3 path, but Lightning just supports local tls files, we need to use a new local dir.
		if err := st.cfg.To.Security.DumpTLSContent("./" + loader.TmpTLSConfigPath + "_" + st.cfg.Name); err != nil {
			return terror.Annotatef(err, "fail to dump tls cert data for lightning, subtask %s ", st.cfg.Name)
		}
	}
	st.units = createUnits(st.cfg, st.etcdClient, st.workerName, relay)
	if len(st.units) < 1 {
		return terror.ErrWorkerNoAvailUnits.Generate(st.cfg.Name, st.cfg.Mode)
	}

	initializeUnitSuccess := true
	// when error occurred, initialized units should be closed
	// when continue sub task from loader / syncer, ahead units should be closed
	var needCloseUnits []unit.Unit
	defer func() {
		for _, u := range needCloseUnits {
			u.Close()
		}

		st.initialized.Store(initializeUnitSuccess)
	}()

	// every unit does base initialization in `Init`, and this must pass before start running the sub task
	// other setups can be done in `Process`, like Loader's prepare which depends on Mydumper's output
	// but setups in `Process` should be treated carefully, let it's compatible with Pause / Resume
	for i, u := range st.units {
		ctx, cancel := context.WithTimeout(context.Background(), unit.DefaultInitTimeout)
		err := u.Init(ctx)
		cancel()
		if err != nil {
			initializeUnitSuccess = false
			// when init fail, other units initialized before should be closed
			for j := 0; j < i; j++ {
				needCloseUnits = append(needCloseUnits, st.units[j])
			}
			return terror.Annotatef(err, "fail to initialize unit %s of subtask %s ", u.Type(), st.cfg.Name)
		}
	}

	// if the sub task ran before, some units may be skipped
	skipIdx := 0
	for i := len(st.units) - 1; i > 0; i-- {
		u := st.units[i]
		ctx, cancel := context.WithTimeout(context.Background(), unit.DefaultInitTimeout)
		isFresh, err := u.IsFreshTask(ctx)
		cancel()
		if err != nil {
			initializeUnitSuccess = false
			return terror.Annotatef(err, "fail to get fresh status of subtask %s %s", st.cfg.Name, u.Type())
		} else if !isFresh {
			skipIdx = i
			st.l.Info("continue unit", zap.Stringer("unit", u.Type()))
			break
		}
	}

	needCloseUnits = st.units[:skipIdx]
	st.units = st.units[skipIdx:]

	st.setCurrUnit(st.units[0])
	return nil
}

// Run runs the sub task.
// TODO: check concurrent problems.
func (st *SubTask) Run(expectStage pb.Stage, expectValidatorStage pb.Stage, relay relay.Process) {
	if st.Stage() == pb.Stage_Finished || st.Stage() == pb.Stage_Running {
		st.l.Warn("prepare to run a subtask with invalid stage",
			zap.Stringer("current stage", st.Stage()),
			zap.Stringer("expected stage", expectStage))
		return
	}

	if err := st.initUnits(relay); err != nil {
		st.l.Error("fail to initialize subtask", log.ShortError(err))
		st.fail(err)
		return
	}

	st.StartValidator(expectValidatorStage, true)

	if expectStage == pb.Stage_Running {
		st.run()
	} else {
		// if not want to run, still need to set the stage.
		st.setStage(expectStage)
	}
}

func (st *SubTask) run() {
	st.setStageAndResult(pb.Stage_Running, nil) // clear previous result
	ctx, cancel := context.WithCancel(st.ctx)
	st.setCurrCtx(ctx, cancel)
	err := st.unitTransWaitCondition(ctx)
	if err != nil {
		st.l.Error("wait condition", log.ShortError(err))
		st.fail(err)
		return
	} else if ctx.Err() != nil {
		st.l.Error("exit SubTask.run", log.ShortError(ctx.Err()))
		return
	}

	cu := st.CurrUnit()
	st.l.Info("start to run", zap.Stringer("unit", cu.Type()))
	pr := make(chan pb.ProcessResult, 1)
	st.resultWg.Add(1)
	go st.fetchResultAndUpdateStage(pr)
	go cu.Process(ctx, pr)
}

func (st *SubTask) StartValidator(expect pb.Stage, startWithSubtask bool) {
	// when validator mode=none
	if expect == pb.Stage_InvalidStage {
		return
	}
	st.Lock()
	defer st.Unlock()

	if st.cfg.ValidatorCfg.Mode != config.ValidationFast && st.cfg.ValidatorCfg.Mode != config.ValidationFull {
		return
	}
	var syncerObj *syncer.Syncer
	var ok bool
	for _, u := range st.units {
		if syncerObj, ok = u.(*syncer.Syncer); ok {
			break
		}
	}
	if syncerObj == nil {
		st.l.Warn("cannot start validator without syncer")
		return
	}
	if st.validator == nil {
		st.validator = syncer.NewContinuousDataValidator(st.cfg, syncerObj, startWithSubtask)
	}
	st.validator.Start(expect)
}

func (st *SubTask) StopValidator() {
	st.Lock()
	if st.validator != nil {
		st.validator.Stop()
	}
	st.Unlock()
}

func (st *SubTask) setCurrCtx(ctx context.Context, cancel context.CancelFunc) {
	st.Lock()
	// call previous cancel func for safety
	if st.currCancel != nil {
		st.currCancel()
	}
	st.currCtx = ctx
	st.currCancel = cancel
	st.Unlock()
}

func (st *SubTask) callCurrCancel() {
	st.RLock()
	st.currCancel()
	st.RUnlock()
}

// fetchResultAndUpdateStage fetches process result, call Pause of current unit if needed and updates the stage of subtask.
func (st *SubTask) fetchResultAndUpdateStage(pr chan pb.ProcessResult) {
	defer st.resultWg.Done()

	result := <-pr
	// filter the context canceled error
	errs := make([]*pb.ProcessError, 0, 2)
	for _, err := range result.Errors {
		if !unit.IsCtxCanceledProcessErr(err) {
			errs = append(errs, err)
		}
	}
	result.Errors = errs

	st.callCurrCancel() // dm-unit finished, canceled or error occurred, always cancel processing

	var (
		cu    = st.CurrUnit()
		stage pb.Stage
	)

	// update the stage according to result
	if len(result.Errors) == 0 {
		switch st.Stage() {
		case pb.Stage_Pausing:
			// paused by st.Pause
			stage = pb.Stage_Paused
		case pb.Stage_Stopping:
			// stopped by st.Close
			stage = pb.Stage_Stopped
		default:
			// process finished with no error
			stage = pb.Stage_Finished
		}
	} else {
		// error occurred, paused
		stage = pb.Stage_Paused
	}
	st.setStageAndResult(stage, &result)

	st.l.Info("unit process returned", zap.Stringer("unit", cu.Type()), zap.Stringer("stage", stage), zap.String("status", st.StatusJSON()))

	switch stage {
	case pb.Stage_Finished:
		cu.Close()
		nu := st.getNextUnit()
		if nu == nil {
			// Now, when finished, it only stops the process
			// if needed, we can refine to Close it
			st.l.Info("all process units finished")
		} else {
			st.l.Info("switching to next unit", zap.Stringer("unit", cu.Type()))
			st.setCurrUnit(nu)
			// NOTE: maybe need a Lock mechanism for sharding scenario
			st.run() // re-run for next process unit
		}
	case pb.Stage_Stopped:
		// the caller will close current unit and more units after it, so we don't call cu.Close here.
	case pb.Stage_Paused:
		cu.Pause()
		for _, err := range result.Errors {
			st.l.Error("unit process error", zap.Stringer("unit", cu.Type()), zap.Any("error information", err))
		}
		st.l.Info("paused", zap.Stringer("unit", cu.Type()))
	}
}

// setCurrUnit set current dm unit to ut.
func (st *SubTask) setCurrUnit(cu unit.Unit) {
	st.Lock()
	defer st.Unlock()
	pu := st.currUnit
	st.currUnit = cu
	st.prevUnit = pu
}

// CurrUnit returns current dm unit.
func (st *SubTask) CurrUnit() unit.Unit {
	st.RLock()
	defer st.RUnlock()
	return st.currUnit
}

// PrevUnit returns dm previous unit.
func (st *SubTask) PrevUnit() unit.Unit {
	st.RLock()
	defer st.RUnlock()
	return st.prevUnit
}

// closeUnits closes all un-closed units (current unit and all the subsequent units).
func (st *SubTask) closeUnits() {
	st.cancel()
	st.resultWg.Wait()

	var (
		cu  = st.currUnit
		cui = -1
	)

	for i, u := range st.units {
		if u == cu {
			cui = i
			break
		}
	}
	if cui < 0 {
		return
	}

	for i := cui; i < len(st.units); i++ {
		u := st.units[i]
		st.l.Info("closing unit process", zap.Stringer("unit", cu.Type()))
		u.Close()
		st.l.Info("closing unit done", zap.Stringer("unit", cu.Type()))
	}
}

func (st *SubTask) killCurrentUnit() {
	if st.CurrUnit() != nil {
		ut := st.CurrUnit().Type()
		st.l.Info("kill unit", zap.String("task", st.cfg.Name), zap.Stringer("unit", ut))
		st.CurrUnit().Kill()
		st.l.Info("kill unit done", zap.String("task", st.cfg.Name), zap.Stringer("unit", ut))
	}
}

// getNextUnit gets the next process unit from st.units
// if no next unit, return nil.
func (st *SubTask) getNextUnit() unit.Unit {
	var (
		nu  unit.Unit
		cui = len(st.units)
		cu  = st.CurrUnit()
	)
	for i, u := range st.units {
		if u == cu {
			cui = i
		}
		if i == cui+1 {
			nu = u
			break
		}
	}
	return nu
}

func (st *SubTask) setStage(stage pb.Stage) {
	st.Lock()
	defer st.Unlock()
	st.stage = stage
	updateTaskMetric(st.cfg.Name, st.cfg.SourceID, st.stage, st.workerName)
}

func (st *SubTask) setStageAndResult(stage pb.Stage, result *pb.ProcessResult) {
	st.Lock()
	defer st.Unlock()
	st.stage = stage
	updateTaskMetric(st.cfg.Name, st.cfg.SourceID, st.stage, st.workerName)
	st.result = result
}

// stageCAS sets stage to newStage if its current value is oldStage.
func (st *SubTask) stageCAS(oldStage, newStage pb.Stage) bool {
	st.Lock()
	defer st.Unlock()

	if st.stage == oldStage {
		st.stage = newStage
		updateTaskMetric(st.cfg.Name, st.cfg.SourceID, st.stage, st.workerName)
		return true
	}
	return false
}

// setStageIfNotIn sets stage to newStage if its current value is not in oldStages.
func (st *SubTask) setStageIfNotIn(oldStages []pb.Stage, newStage pb.Stage) bool {
	st.Lock()
	defer st.Unlock()
	for _, s := range oldStages {
		if st.stage == s {
			return false
		}
	}
	st.stage = newStage
	updateTaskMetric(st.cfg.Name, st.cfg.SourceID, st.stage, st.workerName)
	return true
}

// setStageIfNotIn sets stage to newStage if its current value is in oldStages.
func (st *SubTask) setStageIfIn(oldStages []pb.Stage, newStage pb.Stage) bool {
	st.Lock()
	defer st.Unlock()
	for _, s := range oldStages {
		if st.stage == s {
			st.stage = newStage
			updateTaskMetric(st.cfg.Name, st.cfg.SourceID, st.stage, st.workerName)
			return true
		}
	}
	return false
}

// Stage returns the stage of the sub task.
func (st *SubTask) Stage() pb.Stage {
	st.RLock()
	defer st.RUnlock()
	return st.stage
}

func (st *SubTask) validatorStage() pb.Stage {
	st.RLock()
	defer st.RUnlock()
	if st.validator != nil {
		return st.validator.Stage()
	}
	return pb.Stage_InvalidStage
}

// markResultCanceled mark result as canceled if stage is Paused.
// This func is used to pause a task which has been paused by error,
// so the task will not auto resume by task checker.
func (st *SubTask) markResultCanceled() bool {
	st.Lock()
	defer st.Unlock()
	if st.stage == pb.Stage_Paused {
		if st.result != nil && !st.result.IsCanceled {
			st.l.Info("manually pause task which has been paused by errors")
			st.result.IsCanceled = true
			return true
		}
	}
	return false
}

// Result returns the result of the sub task.
func (st *SubTask) Result() *pb.ProcessResult {
	st.RLock()
	defer st.RUnlock()
	if st.result == nil {
		return nil
	}
	tempProcessResult, _ := st.result.Marshal()
	newProcessResult := &pb.ProcessResult{}
	_ = newProcessResult.Unmarshal(tempProcessResult)
	return newProcessResult
}

// Close stops the sub task.
func (st *SubTask) Close() {
	st.l.Info("closing")
	if !st.setStageIfNotIn([]pb.Stage{pb.Stage_Stopped, pb.Stage_Stopping, pb.Stage_Finished}, pb.Stage_Stopping) {
		st.l.Info("subTask is already closed, no need to close")
		return
	}
	st.closeUnits() // close all un-closed units
	updateTaskMetric(st.cfg.Name, st.cfg.SourceID, pb.Stage_Stopped, st.workerName)

	// we can start/stop validator independent of task, so we don't set st.validator = nil inside
	st.StopValidator()
	st.validator = nil
}

// Kill kill running unit and stop the sub task.
func (st *SubTask) Kill() {
	st.l.Info("killing")
	if !st.setStageIfNotIn([]pb.Stage{pb.Stage_Stopped, pb.Stage_Stopping, pb.Stage_Finished}, pb.Stage_Stopping) {
		st.l.Info("subTask is already closed, no need to close")
		return
	}
	st.killCurrentUnit()
	st.closeUnits() // close all un-closed units

	cfg := st.getCfg()
	updateTaskMetric(cfg.Name, cfg.SourceID, pb.Stage_Stopped, st.workerName)

	st.StopValidator()
	st.validator = nil
}

// Pause pauses a running sub task or a sub task paused by error.
func (st *SubTask) Pause() error {
	if st.markResultCanceled() {
		return nil
	}

	if !st.stageCAS(pb.Stage_Running, pb.Stage_Pausing) {
		return terror.ErrWorkerNotRunningStage.Generate(st.Stage().String())
	}

	st.callCurrCancel()
	st.resultWg.Wait() // wait fetchResultAndUpdateStage set Pause stage

	return nil
}

// Resume resumes the paused sub task
// TODO: similar to Run, refactor later.
func (st *SubTask) Resume(relay relay.Process) error {
	if !st.initialized.Load() {
		expectValidatorStage, err := getExpectValidatorStage(st.cfg.ValidatorCfg, st.etcdClient, st.cfg.SourceID, st.cfg.Name, 0)
		if err != nil {
			return terror.Annotate(err, "fail to get validator stage from etcd")
		}
		st.Run(pb.Stage_Running, expectValidatorStage, relay)
		return nil
	}

	if !st.setStageIfIn([]pb.Stage{pb.Stage_Paused, pb.Stage_Stopped}, pb.Stage_Resuming) {
		return terror.ErrWorkerNotPausedStage.Generate(st.Stage().String())
	}

	ctx, cancel := context.WithCancel(st.ctx)
	st.setCurrCtx(ctx, cancel)
	// NOTE: this may block if user resume a task
	err := st.unitTransWaitCondition(ctx)
	if err != nil {
		st.l.Error("wait condition", log.ShortError(err))
		st.fail(err)
		return err
	} else if ctx.Err() != nil {
		// ctx.Err() != nil means this context is canceled in other go routine,
		// that go routine will change the stage, so don't need to set stage to paused here.
		// nolint:nilerr
		return nil
	}

	cu := st.CurrUnit()
	st.l.Info("resume with unit", zap.Stringer("unit", cu.Type()))

	pr := make(chan pb.ProcessResult, 1)
	st.resultWg.Add(1)
	go st.fetchResultAndUpdateStage(pr)
	go cu.Resume(ctx, pr)

	st.setStageAndResult(pb.Stage_Running, nil) // clear previous result
	return nil
}

// Update update the sub task's config.
func (st *SubTask) Update(ctx context.Context, cfg *config.SubTaskConfig) error {
	if !st.stageCAS(pb.Stage_Paused, pb.Stage_Paused) { // only test for Paused
		return terror.ErrWorkerUpdateTaskStage.Generate(st.Stage().String())
	}

	for _, u := range st.units {
		err := u.Update(ctx, cfg)
		if err != nil {
			return err
		}
	}
	st.SetCfg(*cfg)
	return nil
}

// OperateSchema operates schema for an upstream table.
func (st *SubTask) OperateSchema(ctx context.Context, req *pb.OperateWorkerSchemaRequest) (schema string, err error) {
	if st.Stage() != pb.Stage_Paused && req.Op != pb.SchemaOp_ListMigrateTargets {
		return "", terror.ErrWorkerNotPausedStage.Generate(st.Stage().String())
	}

	syncUnit, ok := st.currUnit.(*syncer.Syncer)
	if !ok {
		return "", terror.ErrWorkerOperSyncUnitOnly.Generate(st.currUnit.Type())
	}

	if st.validatorStage() == pb.Stage_Running && req.Op != pb.SchemaOp_ListMigrateTargets {
		return "", terror.ErrWorkerValidatorNotPaused.Generate(pb.Stage_Running.String())
	}

	return syncUnit.OperateSchema(ctx, req)
}

// UpdateFromConfig updates config for `From`.
func (st *SubTask) UpdateFromConfig(cfg *config.SubTaskConfig) error {
	st.Lock()
	defer st.Unlock()

	if sync, ok := st.currUnit.(*syncer.Syncer); ok {
		err := sync.UpdateFromConfig(cfg)
		if err != nil {
			return err
		}
	}

	st.cfg.From = cfg.From

	return nil
}

// CheckUnit checks whether current unit is sync unit.
func (st *SubTask) CheckUnit() bool {
	st.RLock()
	defer st.RUnlock()
	flag := true
	if _, ok := st.currUnit.(*syncer.Syncer); !ok {
		flag = false
	}
	return flag
}

// CheckUnitCfgCanUpdate checks this unit cfg can update.
func (st *SubTask) CheckUnitCfgCanUpdate(cfg *config.SubTaskConfig) error {
	st.RLock()
	defer st.RUnlock()

	if st.currUnit == nil {
		return terror.ErrWorkerUpdateSubTaskConfig.Generate(cfg.Name, pb.UnitType_InvalidUnit)
	}

	switch st.currUnit.Type() {
	case pb.UnitType_Sync:
		if s, ok := st.currUnit.(*syncer.Syncer); ok {
			return s.CheckCanUpdateCfg(cfg)
		}
		// skip check for mock sync unit
	default:
		return terror.ErrWorkerUpdateSubTaskConfig.Generate(cfg.Name, st.currUnit.Type())
	}
	return nil
}

// ShardDDLOperation returns the current shard DDL lock operation.
func (st *SubTask) ShardDDLOperation() *pessimism.Operation {
	st.RLock()
	defer st.RUnlock()

	cu := st.currUnit
	syncer2, ok := cu.(*syncer.Syncer)
	if !ok {
		return nil
	}

	return syncer2.ShardDDLOperation()
}

// unitTransWaitCondition waits when transferring from current unit to next unit.
// Currently there is only one wait condition
// from Load unit to Sync unit, wait for relay-log catched up with mydumper binlog position.
func (st *SubTask) unitTransWaitCondition(subTaskCtx context.Context) error {
	var (
		gset1 mysql.GTIDSet
		gset2 mysql.GTIDSet
		pos1  *mysql.Position
		pos2  *mysql.Position
		err   error
	)
	pu := st.PrevUnit()
	cu := st.CurrUnit()
	if pu != nil && pu.Type() == pb.UnitType_Load && cu.Type() == pb.UnitType_Sync {
		st.l.Info("wait condition between two units", zap.Stringer("previous unit", pu.Type()), zap.Stringer("unit", cu.Type()))
		hub := GetConditionHub()

		if !hub.w.relayEnabled.Load() {
			return nil
		}

		ctxWait, cancelWait := context.WithTimeout(hub.w.ctx, waitRelayCatchupTimeout)
		defer cancelWait()

		loadStatus := pu.Status(nil).(*pb.LoadStatus)

		cfg := st.getCfg()
		if cfg.EnableGTID {
			gset1, err = gtid.ParserGTID(cfg.Flavor, loadStatus.MetaBinlogGTID)
			if err != nil {
				return terror.WithClass(err, terror.ClassDMWorker)
			}
		} else {
			pos1, err = utils.DecodeBinlogPosition(loadStatus.MetaBinlog)
			if err != nil {
				return terror.WithClass(err, terror.ClassDMWorker)
			}
		}

		for {
			relayStatus := hub.w.relayHolder.Status(nil)

			if cfg.EnableGTID {
				gset2, err = gtid.ParserGTID(cfg.Flavor, relayStatus.RelayBinlogGtid)
				if err != nil {
					return terror.WithClass(err, terror.ClassDMWorker)
				}
				rc, ok := binlog.CompareGTID(gset1, gset2)
				if !ok {
					return terror.ErrWorkerWaitRelayCatchupGTID.Generate(loadStatus.MetaBinlogGTID, relayStatus.RelayBinlogGtid)
				}
				if rc <= 0 {
					break
				}
			} else {
				pos2, err = utils.DecodeBinlogPosition(relayStatus.RelayBinlog)
				if err != nil {
					return terror.WithClass(err, terror.ClassDMWorker)
				}
				if pos1.Compare(*pos2) <= 0 {
					break
				}
			}

			st.l.Debug("wait relay to catchup", zap.Bool("enableGTID", cfg.EnableGTID), zap.Stringer("load end position", pos1), zap.String("load end gtid", loadStatus.MetaBinlogGTID), zap.Stringer("relay position", pos2), zap.String("relay gtid", relayStatus.RelayBinlogGtid))

			select {
			case <-ctxWait.Done():
				if cfg.EnableGTID {
					return terror.ErrWorkerWaitRelayCatchupTimeout.Generate(waitRelayCatchupTimeout, loadStatus.MetaBinlogGTID, relayStatus.RelayBinlogGtid)
				}
				return terror.ErrWorkerWaitRelayCatchupTimeout.Generate(waitRelayCatchupTimeout, pos1, pos2)
			case <-subTaskCtx.Done():
				return nil
			case <-time.After(time.Millisecond * 50):
			}
		}
		st.l.Info("relay binlog pos catchup loader end binlog pos")
	}
	return nil
}

func (st *SubTask) fail(err error) {
	st.setStageAndResult(pb.Stage_Paused, &pb.ProcessResult{
		Errors: []*pb.ProcessError{
			unit.NewProcessError(err),
		},
	})
}

// HandleError handle error for syncer unit.
func (st *SubTask) HandleError(ctx context.Context, req *pb.HandleWorkerErrorRequest, relay relay.Process) (string, error) {
	syncUnit, ok := st.currUnit.(*syncer.Syncer)
	if !ok {
		return "", terror.ErrWorkerOperSyncUnitOnly.Generate(st.currUnit.Type())
	}

	msg, err := syncUnit.HandleError(ctx, req)
	if err != nil {
		return "", err
	}

	if st.Stage() == pb.Stage_Paused && req.Op != pb.ErrorOp_List {
		err = st.Resume(relay)
	}
	return msg, err
}

func (st *SubTask) getCfg() *config.SubTaskConfig {
	st.RLock()
	defer st.RUnlock()
	return st.cfg
}

func (st *SubTask) SetCfg(subTaskConfig config.SubTaskConfig) {
	st.Lock()
	st.cfg = &subTaskConfig
	st.Unlock()
}

func (st *SubTask) UpdateValidatorCfg(validatorCfg config.ValidatorConfig) {
	st.Lock()
	// if user start validator on the fly, we update validator mode and start-time
	st.cfg.ValidatorCfg.Mode = validatorCfg.Mode
	st.cfg.ValidatorCfg.StartTime = validatorCfg.StartTime
	st.Unlock()
}

func (st *SubTask) getValidatorStage() pb.Stage {
	st.RLock()
	defer st.RUnlock()

	if st.validator != nil {
		return st.validator.Stage()
	}
	return pb.Stage_InvalidStage
}

func updateTaskMetric(task, sourceID string, stage pb.Stage, workerName string) {
	if stage == pb.Stage_Stopped || stage == pb.Stage_Finished {
		taskState.DeleteAllAboutLabels(prometheus.Labels{"task": task, "source_id": sourceID})
	} else {
		taskState.WithLabelValues(task, sourceID, workerName).Set(float64(stage))
	}
}

func (st *SubTask) GetValidatorError(errState pb.ValidateErrorState) ([]*pb.ValidationError, error) {
	if validator := st.getValidator(); validator != nil {
		return validator.GetValidatorError(errState)
	}
	cfg := st.getCfg()
	return nil, terror.ErrValidatorNotFound.Generate(cfg.Name, cfg.SourceID)
}

func (st *SubTask) OperateValidatorError(op pb.ValidationErrOp, errID uint64, isAll bool) error {
	if validator := st.getValidator(); validator != nil {
		return validator.OperateValidatorError(op, errID, isAll)
	}
	cfg := st.getCfg()
	return terror.ErrValidatorNotFound.Generate(cfg.Name, cfg.SourceID)
}

func (st *SubTask) getValidator() *syncer.DataValidator {
	st.RLock()
	defer st.RUnlock()
	return st.validator
}

func (st *SubTask) GetValidatorStatus() (*pb.ValidationStatus, error) {
	validator := st.getValidator()
	if validator == nil {
		cfg := st.getCfg()
		return nil, terror.ErrValidatorNotFound.Generate(cfg.Name, cfg.SourceID)
	}
	return validator.GetValidatorStatus(), nil
}

func (st *SubTask) GetValidatorTableStatus(filterStatus pb.Stage) ([]*pb.ValidationTableStatus, error) {
	validator := st.getValidator()
	if validator == nil {
		cfg := st.getCfg()
		return nil, terror.ErrValidatorNotFound.Generate(cfg.Name, cfg.SourceID)
	}
	return validator.GetValidatorTableStatus(filterStatus), nil
}
