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
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/config/dbconfig"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/log"
	pkgstreamer "github.com/pingcap/tiflow/dm/pkg/streamer"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/relay"
	"github.com/pingcap/tiflow/dm/unit"
)

type testRelay struct{}

var _ = check.Suite(&testRelay{})

/*********** dummy relay log process unit, used only for testing *************/

// DummyRelay is a dummy relay.
type DummyRelay struct {
	initErr error

	processResult pb.ProcessResult
	errorInfo     *pb.RelayError
	reloadErr     error
}

func (d *DummyRelay) IsActive(uuid, filename string) (bool, int64) {
	return false, 0
}

func (d *DummyRelay) NewReader(logger log.Logger, cfg *relay.BinlogReaderConfig) *relay.BinlogReader {
	return nil
}

func (d *DummyRelay) RegisterListener(el relay.Listener) {
}

func (d *DummyRelay) UnRegisterListener(el relay.Listener) {
}

// NewDummyRelay creates an instance of dummy Relay.
func NewDummyRelay(cfg *relay.Config) relay.Process {
	return &DummyRelay{}
}

// Init implements Process interface.
func (d *DummyRelay) Init(ctx context.Context) error {
	return d.initErr
}

// InjectInitError injects init error.
func (d *DummyRelay) InjectInitError(err error) {
	d.initErr = err
}

// Process implements Process interface.
func (d *DummyRelay) Process(ctx context.Context) pb.ProcessResult {
	<-ctx.Done()
	return d.processResult
}

// InjectProcessResult injects process result.
func (d *DummyRelay) InjectProcessResult(result pb.ProcessResult) {
	d.processResult = result
}

// ActiveRelayLog implements Process interface.
func (d *DummyRelay) ActiveRelayLog() *pkgstreamer.RelayLogInfo {
	return nil
}

// Reload implements Process interface.
func (d *DummyRelay) Reload(newCfg *relay.Config) error {
	return d.reloadErr
}

// InjectReloadError injects reload error.
func (d *DummyRelay) InjectReloadError(err error) {
	d.reloadErr = err
}

// Update implements Process interface.
func (d *DummyRelay) Update(cfg *config.SubTaskConfig) error {
	return nil
}

// Resume implements Process interface.
func (d *DummyRelay) Resume(ctx context.Context, pr chan pb.ProcessResult) {}

// Pause implements Process interface.
func (d *DummyRelay) Pause() {}

// Error implements Process interface.
func (d *DummyRelay) Error() interface{} {
	return d.errorInfo
}

// Status implements Process interface.
func (d *DummyRelay) Status(sourceStatus *binlog.SourceStatus) interface{} {
	return &pb.RelayStatus{
		Stage: pb.Stage_New,
	}
}

// Close implements Process interface.
func (d *DummyRelay) Close() {}

// IsClosed implements Process interface.
func (d *DummyRelay) IsClosed() bool { return false }

// SaveMeta implements Process interface.
func (d *DummyRelay) SaveMeta(pos mysql.Position, gset mysql.GTIDSet) error {
	return nil
}

// ResetMeta implements Process interface.
func (d *DummyRelay) ResetMeta() {}

// PurgeRelayDir implements Process interface.
func (d *DummyRelay) PurgeRelayDir() error {
	return nil
}

func (t *testRelay) TestRelay(c *check.C) {
	originNewRelay := relay.NewRelay
	relay.NewRelay = NewDummyRelay
	originNewPurger := relay.NewPurger
	relay.NewPurger = relay.NewDummyPurger
	defer func() {
		relay.NewRelay = originNewRelay
		relay.NewPurger = originNewPurger
	}()

	cfg := loadSourceConfigWithoutPassword(c)

	dir := c.MkDir()
	cfg.RelayDir = dir
	cfg.MetaDir = dir

	relayHolder := NewRealRelayHolder(cfg)
	c.Assert(relayHolder, check.NotNil)

	holder, ok := relayHolder.(*realRelayHolder)
	c.Assert(ok, check.IsTrue)

	t.testInit(c, holder)
	t.testStart(c, holder)
	t.testPauseAndResume(c, holder)
	t.testClose(c, holder)
	t.testStop(c, holder)
}

func (t *testRelay) testInit(c *check.C, holder *realRelayHolder) {
	ctx := context.Background()
	_, err := holder.Init(ctx, nil)
	c.Assert(err, check.IsNil)

	r, ok := holder.relay.(*DummyRelay)
	c.Assert(ok, check.IsTrue)

	initErr := errors.New("init error")
	r.InjectInitError(initErr)
	defer r.InjectInitError(nil)

	_, err = holder.Init(ctx, nil)
	c.Assert(err, check.ErrorMatches, ".*"+initErr.Error()+".*")
}

func (t *testRelay) testStart(c *check.C, holder *realRelayHolder) {
	c.Assert(holder.Stage(), check.Equals, pb.Stage_New)
	c.Assert(holder.closed.Load(), check.IsFalse)
	c.Assert(holder.Result(), check.IsNil)

	holder.Start()
	c.Assert(waitRelayStage(holder, pb.Stage_Running, 5), check.IsTrue)
	c.Assert(holder.Result(), check.IsNil)
	c.Assert(holder.closed.Load(), check.IsFalse)

	// test status
	status := holder.Status(nil)
	c.Assert(status.Stage, check.Equals, pb.Stage_Running)
	c.Assert(status.Result, check.IsNil)

	c.Assert(holder.Error(), check.IsNil)

	// test update and pause -> resume
	t.testUpdate(c, holder)
	c.Assert(holder.Stage(), check.Equals, pb.Stage_Paused)
	c.Assert(holder.closed.Load(), check.IsFalse)

	err := holder.Operate(context.Background(), pb.RelayOp_ResumeRelay)
	c.Assert(err, check.IsNil)
	c.Assert(waitRelayStage(holder, pb.Stage_Running, 10), check.IsTrue)
	c.Assert(holder.Result(), check.IsNil)
	c.Assert(holder.closed.Load(), check.IsFalse)
}

func (t *testRelay) testClose(c *check.C, holder *realRelayHolder) {
	r, ok := holder.relay.(*DummyRelay)
	c.Assert(ok, check.IsTrue)
	processResult := &pb.ProcessResult{
		IsCanceled: true,
		Errors: []*pb.ProcessError{
			unit.NewProcessError(errors.New("process error")),
		},
	}
	r.InjectProcessResult(*processResult)
	defer r.InjectProcessResult(pb.ProcessResult{})

	holder.Close()
	c.Assert(waitRelayStage(holder, pb.Stage_Paused, 10), check.IsTrue)
	c.Assert(holder.Result(), check.DeepEquals, processResult)
	c.Assert(holder.closed.Load(), check.IsTrue)

	holder.Close()
	c.Assert(holder.Stage(), check.Equals, pb.Stage_Paused)
	c.Assert(holder.Result(), check.DeepEquals, processResult)
	c.Assert(holder.closed.Load(), check.IsTrue)

	// todo: very strange, and can't resume
	status := holder.Status(nil)
	c.Assert(status.Stage, check.Equals, pb.Stage_Stopped)
	c.Assert(status.Result, check.IsNil)

	errInfo := holder.Error()
	c.Assert(errInfo.Msg, check.Equals, "relay stopped")
}

func (t *testRelay) testPauseAndResume(c *check.C, holder *realRelayHolder) {
	err := holder.Operate(context.Background(), pb.RelayOp_PauseRelay)
	c.Assert(err, check.IsNil)
	c.Assert(holder.Stage(), check.Equals, pb.Stage_Paused)
	c.Assert(holder.closed.Load(), check.IsFalse)

	err = holder.pauseRelay(context.Background(), pb.RelayOp_PauseRelay)
	c.Assert(err, check.ErrorMatches, ".*current stage is Paused.*")

	// test status
	status := holder.Status(nil)
	c.Assert(status.Stage, check.Equals, pb.Stage_Paused)

	// test update
	t.testUpdate(c, holder)

	err = holder.Operate(context.Background(), pb.RelayOp_ResumeRelay)
	c.Assert(err, check.IsNil)
	c.Assert(waitRelayStage(holder, pb.Stage_Running, 10), check.IsTrue)
	c.Assert(holder.Result(), check.IsNil)
	c.Assert(holder.closed.Load(), check.IsFalse)

	err = holder.Operate(context.Background(), pb.RelayOp_ResumeRelay)
	c.Assert(err, check.ErrorMatches, ".*current stage is Running.*")

	// test status
	status = holder.Status(nil)
	c.Assert(status.Stage, check.Equals, pb.Stage_Running)
	c.Assert(status.Result, check.IsNil)

	// invalid operation
	err = holder.Operate(context.Background(), pb.RelayOp_InvalidRelayOp)
	c.Assert(err, check.ErrorMatches, ".*not supported.*")
}

func (t *testRelay) testUpdate(c *check.C, holder *realRelayHolder) {
	cfg := &config.SourceConfig{
		From: dbconfig.DBConfig{
			Host:     "127.0.0.1",
			Port:     3306,
			User:     "root",
			Password: "1234",
		},
	}

	originStage := holder.Stage()
	c.Assert(holder.Update(context.Background(), cfg), check.IsNil)
	c.Assert(waitRelayStage(holder, originStage, 10), check.IsTrue)
	c.Assert(holder.closed.Load(), check.IsFalse)

	r, ok := holder.relay.(*DummyRelay)
	c.Assert(ok, check.IsTrue)

	err := errors.New("reload error")
	r.InjectReloadError(err)
	defer r.InjectReloadError(nil)
	c.Assert(holder.Update(context.Background(), cfg), check.Equals, err)
}

func (t *testRelay) testStop(c *check.C, holder *realRelayHolder) {
	err := holder.Operate(context.Background(), pb.RelayOp_StopRelay)
	c.Assert(err, check.IsNil)
	c.Assert(holder.Stage(), check.Equals, pb.Stage_Stopped)
	c.Assert(holder.closed.Load(), check.IsTrue)

	err = holder.Operate(context.Background(), pb.RelayOp_StopRelay)
	c.Assert(err, check.ErrorMatches, ".*current stage is already stopped.*")
}

func waitRelayStage(holder *realRelayHolder, expect pb.Stage, backoff int) bool {
	return utils.WaitSomething(backoff, 10*time.Millisecond, func() bool {
		return holder.Stage() == expect
	})
}
