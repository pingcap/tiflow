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
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
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
	"github.com/stretchr/testify/require"
)

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

func TestRelay(t *testing.T) {
	originNewRelay := relay.NewRelay
	relay.NewRelay = NewDummyRelay
	originNewPurger := relay.NewPurger
	relay.NewPurger = relay.NewDummyPurger
	defer func() {
		relay.NewRelay = originNewRelay
		relay.NewPurger = originNewPurger
	}()

	cfg := loadSourceConfigWithoutPassword(t)

	dir := t.TempDir()
	cfg.RelayDir = dir
	cfg.MetaDir = dir

	relayHolder := NewRealRelayHolder(cfg)
	require.NotNil(t, relayHolder)

	holder, ok := relayHolder.(*realRelayHolder)
	require.True(t, ok)

	testRelayInit(t, holder)
	testRelayStart(t, holder)
	testRelayPauseAndResume(t, holder)
	testRelayClose(t, holder)
	testRelayStop(t, holder)
}

func testRelayInit(t *testing.T, holder *realRelayHolder) {
	t.Helper()
	ctx := context.Background()
	_, err := holder.Init(ctx, nil)
	require.NoError(t, err)

	r, ok := holder.relay.(*DummyRelay)
	require.True(t, ok)

	initErr := errors.New("init error")
	r.InjectInitError(initErr)
	defer r.InjectInitError(nil)

	_, err = holder.Init(ctx, nil)
	require.Error(t, err)
	require.Regexp(t, ".*"+initErr.Error()+".*", err.Error())
}

func testRelayStart(t *testing.T, holder *realRelayHolder) {
	t.Helper()
	require.Equal(t, pb.Stage_New, holder.Stage())
	require.False(t, holder.closed.Load())
	require.Nil(t, holder.Result())

	holder.Start()
	require.True(t, waitRelayStage(holder, pb.Stage_Running, 5))
	require.Nil(t, holder.Result())
	require.False(t, holder.closed.Load())

	// test status
	status := holder.Status(nil)
	require.Equal(t, pb.Stage_Running, status.Stage)
	require.Nil(t, status.Result)

	require.Nil(t, holder.Error())

	// test update and pause -> resume
	testRelayUpdate(t, holder)
	require.Equal(t, pb.Stage_Paused, holder.Stage())
	require.False(t, holder.closed.Load())

	err := holder.Operate(context.Background(), pb.RelayOp_ResumeRelay)
	require.NoError(t, err)
	require.True(t, waitRelayStage(holder, pb.Stage_Running, 10))
	require.Nil(t, holder.Result())
	require.False(t, holder.closed.Load())
}

func testRelayClose(t *testing.T, holder *realRelayHolder) {
	t.Helper()
	r, ok := holder.relay.(*DummyRelay)
	require.True(t, ok)
	processResult := &pb.ProcessResult{
		IsCanceled: true,
		Errors: []*pb.ProcessError{
			unit.NewProcessError(errors.New("process error")),
		},
	}
	r.InjectProcessResult(*processResult)
	defer r.InjectProcessResult(pb.ProcessResult{})

	holder.Close()
	require.True(t, waitRelayStage(holder, pb.Stage_Paused, 10))
	require.Equal(t, processResult, holder.Result())
	require.True(t, holder.closed.Load())

	holder.Close()
	require.Equal(t, pb.Stage_Paused, holder.Stage())
	require.Equal(t, processResult, holder.Result())
	require.True(t, holder.closed.Load())

	// todo: very strange, and can't resume
	status := holder.Status(nil)
	require.Equal(t, pb.Stage_Stopped, status.Stage)
	require.Nil(t, status.Result)

	errInfo := holder.Error()
	require.Equal(t, "relay stopped", errInfo.Msg)
}

func testRelayPauseAndResume(t *testing.T, holder *realRelayHolder) {
	t.Helper()
	err := holder.Operate(context.Background(), pb.RelayOp_PauseRelay)
	require.NoError(t, err)
	require.Equal(t, pb.Stage_Paused, holder.Stage())
	require.False(t, holder.closed.Load())

	err = holder.pauseRelay(context.Background(), pb.RelayOp_PauseRelay)
	require.Error(t, err)
	require.Regexp(t, ".*current stage is Paused.*", err.Error())

	// test status
	status := holder.Status(nil)
	require.Equal(t, pb.Stage_Paused, status.Stage)

	// test update
	testRelayUpdate(t, holder)

	err = holder.Operate(context.Background(), pb.RelayOp_ResumeRelay)
	require.NoError(t, err)
	require.True(t, waitRelayStage(holder, pb.Stage_Running, 10))
	require.Nil(t, holder.Result())
	require.False(t, holder.closed.Load())

	err = holder.Operate(context.Background(), pb.RelayOp_ResumeRelay)
	require.Error(t, err)
	require.Regexp(t, ".*current stage is Running.*", err.Error())

	// test status
	status = holder.Status(nil)
	require.Equal(t, pb.Stage_Running, status.Stage)
	require.Nil(t, status.Result)

	// invalid operation
	err = holder.Operate(context.Background(), pb.RelayOp_InvalidRelayOp)
	require.Error(t, err)
	require.Regexp(t, ".*not supported.*", err.Error())
}

func testRelayUpdate(t *testing.T, holder *realRelayHolder) {
	t.Helper()
	cfg := &config.SourceConfig{
		From: dbconfig.DBConfig{
			Host:     "127.0.0.1",
			Port:     3306,
			User:     "root",
			Password: "1234",
		},
	}

	originStage := holder.Stage()
	require.NoError(t, holder.Update(context.Background(), cfg))
	require.True(t, waitRelayStage(holder, originStage, 10))
	require.False(t, holder.closed.Load())

	r, ok := holder.relay.(*DummyRelay)
	require.True(t, ok)

	err := errors.New("reload error")
	r.InjectReloadError(err)
	defer r.InjectReloadError(nil)
	require.Equal(t, err, holder.Update(context.Background(), cfg))
}

func testRelayStop(t *testing.T, holder *realRelayHolder) {
	t.Helper()
	err := holder.Operate(context.Background(), pb.RelayOp_StopRelay)
	require.NoError(t, err)
	require.Equal(t, pb.Stage_Stopped, holder.Stage())
	require.True(t, holder.closed.Load())

	err = holder.Operate(context.Background(), pb.RelayOp_StopRelay)
	require.Error(t, err)
	require.Regexp(t, ".*current stage is already stopped.*", err.Error())
}

func waitRelayStage(holder *realRelayHolder, expect pb.Stage, backoff int) bool {
	return utils.WaitSomething(backoff, 10*time.Millisecond, func() bool {
		return holder.Stage() == expect
	})
}
