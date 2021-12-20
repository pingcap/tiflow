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
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/dm/unit"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/log"
	pkgstreamer "github.com/pingcap/tiflow/dm/pkg/streamer"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/relay"
)

type testRelay struct{}

var _ = Suite(&testRelay{})

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
func (d *DummyRelay) SaveMeta(pos mysql.Position, gset gtid.Set) error {
	return nil
}

// ResetMeta implements Process interface.
func (d *DummyRelay) ResetMeta() {}

// PurgeRelayDir implements Process interface.
func (d *DummyRelay) PurgeRelayDir() error {
	return nil
}

func (t *testRelay) TestRelay(c *C) {
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
	c.Assert(relayHolder, NotNil)

	holder, ok := relayHolder.(*realRelayHolder)
	c.Assert(ok, IsTrue)

	t.testInit(c, holder)
	t.testStart(c, holder)
	t.testPauseAndResume(c, holder)
	t.testClose(c, holder)
	t.testStop(c, holder)
}

func (t *testRelay) testInit(c *C, holder *realRelayHolder) {
	ctx := context.Background()
	_, err := holder.Init(ctx, nil)
	c.Assert(err, IsNil)

	r, ok := holder.relay.(*DummyRelay)
	c.Assert(ok, IsTrue)

	initErr := errors.New("init error")
	r.InjectInitError(initErr)
	defer r.InjectInitError(nil)

	_, err = holder.Init(ctx, nil)
	c.Assert(err, ErrorMatches, ".*"+initErr.Error()+".*")
}

func (t *testRelay) testStart(c *C, holder *realRelayHolder) {
	c.Assert(holder.Stage(), Equals, pb.Stage_New)
	c.Assert(holder.closed.Load(), IsFalse)
	c.Assert(holder.Result(), IsNil)

	holder.Start()
	c.Assert(waitRelayStage(holder, pb.Stage_Running, 5), IsTrue)
	c.Assert(holder.Result(), IsNil)
	c.Assert(holder.closed.Load(), IsFalse)

	// test status
	status := holder.Status(nil)
	c.Assert(status.Stage, Equals, pb.Stage_Running)
	c.Assert(status.Result, IsNil)

	c.Assert(holder.Error(), IsNil)

	// test update and pause -> resume
	t.testUpdate(c, holder)
	c.Assert(holder.Stage(), Equals, pb.Stage_Paused)
	c.Assert(holder.closed.Load(), IsFalse)

	err := holder.Operate(context.Background(), pb.RelayOp_ResumeRelay)
	c.Assert(err, IsNil)
	c.Assert(waitRelayStage(holder, pb.Stage_Running, 10), IsTrue)
	c.Assert(holder.Result(), IsNil)
	c.Assert(holder.closed.Load(), IsFalse)
}

func (t *testRelay) testClose(c *C, holder *realRelayHolder) {
	r, ok := holder.relay.(*DummyRelay)
	c.Assert(ok, IsTrue)
	processResult := &pb.ProcessResult{
		IsCanceled: true,
		Errors: []*pb.ProcessError{
			unit.NewProcessError(errors.New("process error")),
		},
	}
	r.InjectProcessResult(*processResult)
	defer r.InjectProcessResult(pb.ProcessResult{})

	holder.Close()
	c.Assert(waitRelayStage(holder, pb.Stage_Paused, 10), IsTrue)
	c.Assert(holder.Result(), DeepEquals, processResult)
	c.Assert(holder.closed.Load(), IsTrue)

	holder.Close()
	c.Assert(holder.Stage(), Equals, pb.Stage_Paused)
	c.Assert(holder.Result(), DeepEquals, processResult)
	c.Assert(holder.closed.Load(), IsTrue)

	// todo: very strange, and can't resume
	status := holder.Status(nil)
	c.Assert(status.Stage, Equals, pb.Stage_Stopped)
	c.Assert(status.Result, IsNil)

	errInfo := holder.Error()
	c.Assert(errInfo.Msg, Equals, "relay stopped")
}

func (t *testRelay) testPauseAndResume(c *C, holder *realRelayHolder) {
	err := holder.Operate(context.Background(), pb.RelayOp_PauseRelay)
	c.Assert(err, IsNil)
	c.Assert(holder.Stage(), Equals, pb.Stage_Paused)
	c.Assert(holder.closed.Load(), IsFalse)

	err = holder.pauseRelay(context.Background(), pb.RelayOp_PauseRelay)
	c.Assert(err, ErrorMatches, ".*current stage is Paused.*")

	// test status
	status := holder.Status(nil)
	c.Assert(status.Stage, Equals, pb.Stage_Paused)

	// test update
	t.testUpdate(c, holder)

	err = holder.Operate(context.Background(), pb.RelayOp_ResumeRelay)
	c.Assert(err, IsNil)
	c.Assert(waitRelayStage(holder, pb.Stage_Running, 10), IsTrue)
	c.Assert(holder.Result(), IsNil)
	c.Assert(holder.closed.Load(), IsFalse)

	err = holder.Operate(context.Background(), pb.RelayOp_ResumeRelay)
	c.Assert(err, ErrorMatches, ".*current stage is Running.*")

	// test status
	status = holder.Status(nil)
	c.Assert(status.Stage, Equals, pb.Stage_Running)
	c.Assert(status.Result, IsNil)

	// invalid operation
	err = holder.Operate(context.Background(), pb.RelayOp_InvalidRelayOp)
	c.Assert(err, ErrorMatches, ".*not supported.*")
}

func (t *testRelay) testUpdate(c *C, holder *realRelayHolder) {
	cfg := &config.SourceConfig{
		From: config.DBConfig{
			Host:     "127.0.0.1",
			Port:     3306,
			User:     "root",
			Password: "1234",
		},
	}

	originStage := holder.Stage()
	c.Assert(holder.Update(context.Background(), cfg), IsNil)
	c.Assert(waitRelayStage(holder, originStage, 10), IsTrue)
	c.Assert(holder.closed.Load(), IsFalse)

	r, ok := holder.relay.(*DummyRelay)
	c.Assert(ok, IsTrue)

	err := errors.New("reload error")
	r.InjectReloadError(err)
	defer r.InjectReloadError(nil)
	c.Assert(holder.Update(context.Background(), cfg), Equals, err)
}

func (t *testRelay) testStop(c *C, holder *realRelayHolder) {
	err := holder.Operate(context.Background(), pb.RelayOp_StopRelay)
	c.Assert(err, IsNil)
	c.Assert(holder.Stage(), Equals, pb.Stage_Stopped)
	c.Assert(holder.closed.Load(), IsTrue)

	err = holder.Operate(context.Background(), pb.RelayOp_StopRelay)
	c.Assert(err, ErrorMatches, ".*current stage is already stopped.*")
}

func waitRelayStage(holder *realRelayHolder, expect pb.Stage, backoff int) bool {
	return utils.WaitSomething(backoff, 10*time.Millisecond, func() bool {
		return holder.Stage() == expect
	})
}
