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

package syncer

import (
	"context"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/dm/unit"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
)

// DataValidator
// validator can be start when there's syncer unit in the subtask and validation mode is not none,
// it's terminated when the subtask is terminated.
// stage of validator is independent of subtask, pause/resume subtask doesn't affect the stage of validator.
//
// validator can be in running or stopped stage
// - in running when it's started with subtask or started later on the fly.
// - in stopped when validation stop is executed.
type DataValidator struct {
	sync.RWMutex
	cfg    *config.SubTaskConfig
	syncer *Syncer

	stage  pb.Stage
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	L                  log.Logger
	fromDB             *conn.BaseDB
	timezone           *time.Location
	syncCfg            replication.BinlogSyncerConfig
	streamerController *StreamerController

	result pb.ProcessResult
}

func NewContinuousDataValidator(cfg *config.SubTaskConfig, syncerObj *Syncer) *DataValidator {
	c := &DataValidator{
		cfg:    cfg,
		syncer: syncerObj,
		stage:  pb.Stage_Stopped,
	}
	c.L = log.With(zap.String("task", cfg.Name), zap.String("unit", "continuous validator"))
	return c
}

func (v *DataValidator) initialize() error {
	newCtx, cancelFunc := context.WithTimeout(v.ctx, unit.DefaultInitTimeout)
	defer cancelFunc()
	tctx := tcontext.NewContext(newCtx, v.L)

	var err error
	defer func() {
		if err != nil && v.fromDB != nil {
			v.fromDB.Close()
		}
	}()

	dbCfg := v.cfg.From
	dbCfg.RawDBCfg = config.DefaultRawDBConfig().SetReadTimeout(maxDMLConnectionTimeout)
	v.fromDB, err = dbconn.CreateBaseDB(&dbCfg)
	if err != nil {
		return err
	}

	v.timezone, err = str2TimezoneOrFromDB(tctx, v.cfg.Timezone, &v.cfg.To)
	if err != nil {
		return err
	}

	v.syncCfg, err = subtaskCfg2BinlogSyncerCfg(v.cfg, v.timezone)
	if err != nil {
		return err
	}

	v.streamerController = NewStreamerController(v.syncCfg, v.cfg.EnableGTID, &dbconn.UpStreamConn{BaseDB: v.fromDB}, v.cfg.RelayDir, v.timezone, nil)

	return nil
}

func (v *DataValidator) Start(expect pb.Stage) {
	v.Lock()
	defer v.Unlock()

	if v.stage == pb.Stage_Running {
		v.L.Info("already started")
		return
	}

	v.ctx, v.cancel = context.WithCancel(context.Background())

	if err := v.initialize(); err != nil {
		v.fillResult(err, false)
		return
	}

	if expect != pb.Stage_Running {
		return
	}

	v.wg.Add(1)
	go func() {
		defer v.wg.Done()
		v.doValidate()
	}()

	v.stage = pb.Stage_Running
}

func (v *DataValidator) fillResult(err error, needLock bool) {
	if needLock {
		v.Lock()
		defer v.Unlock()
	}

	var errs []*pb.ProcessError
	if utils.IsContextCanceledError(err) {
		v.L.Info("filter out context cancelled error", log.ShortError(err))
	} else {
		errs = append(errs, unit.NewProcessError(err))
	}

	isCanceled := false
	select {
	case <-v.ctx.Done():
		isCanceled = true
	default:
	}

	v.result = pb.ProcessResult{
		IsCanceled: isCanceled,
		Errors:     errs,
	}
}

func (v *DataValidator) doValidate() {
	tctx := tcontext.NewContext(v.ctx, v.L)
	err := v.streamerController.Start(tctx, lastLocation)
	if err != nil {
		v.fillResult(terror.Annotate(err, "fail to restart streamer controller"), true)
		return
	}

	v.L.Info("start continuous validation")
}

func (v *DataValidator) Stop() {
	v.Lock()
	defer v.Unlock()
	if v.stage != pb.Stage_Running {
		v.L.Warn("not started")
		return
	}

	v.streamerController.Close()
	v.fromDB.Close()

	if v.cancel != nil {
		v.cancel()
	}
	v.wg.Wait()
	v.stage = pb.Stage_Stopped
}

func (v *DataValidator) Started() bool {
	v.RLock()
	defer v.RUnlock()
	return v.stage == pb.Stage_Running
}

func (v *DataValidator) Stage() pb.Stage {
	v.RLock()
	defer v.RUnlock()
	return v.stage
}
