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
	"io"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/relay"
	"github.com/pingcap/tiflow/dm/unit"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/pd/pkg/utils/tempurl"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/grpc"
)

// do not forget to update this path if the file removed/renamed.
const (
	mydumperPath = "../../bin/mydumper"
)

var etcdErrCompacted = v3rpc.ErrCompacted

// TestWorker is the single entry point that runs all stateful suites in the
// dm/worker package.
func TestWorker(t *testing.T) {
	suite.Run(t, new(testServer))
	suite.Run(t, new(testServer2))
	suite.Run(t, new(testWorkerFunctionalities))
	suite.Run(t, new(testWorkerEtcdCompact))
}

type testServer struct {
	suite.Suite
}

func (t *testServer) SetupSuite() {
	err := log.InitLogger(&log.Config{})
	t.Require().NoError(err)
	getMinLocForSubTaskFunc = getFakeLocForSubTask
}

func (t *testServer) TearDownSuite() {
	getMinLocForSubTaskFunc = getMinLocForSubTask
}

func createMockETCD(dir string, host string) (*embed.Etcd, error) {
	cfg := embed.NewConfig()
	cfg.Dir = dir
	lcurl, _ := url.Parse(host)
	cfg.ListenClientUrls = []url.URL{*lcurl}
	cfg.AdvertiseClientUrls = []url.URL{*lcurl}
	lpurl, _ := url.Parse(tempurl.Alloc())
	cfg.ListenPeerUrls = []url.URL{*lpurl}
	cfg.AdvertisePeerUrls = []url.URL{*lpurl}
	cfg.InitialCluster = "default=" + lpurl.String()
	cfg.Logger = "zap"
	metricsURL, _ := url.Parse(tempurl.Alloc())
	cfg.ListenMetricsUrls = []url.URL{*metricsURL}
	ETCD, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, err
	}

	select {
	case <-ETCD.Server.ReadyNotify():
	case <-time.After(5 * time.Second):
		ETCD.Server.Stop() // trigger a shutdown
	}
	// embd.client = v3client.New(embd.ETCD.Server)
	return ETCD, nil
}

func (t *testServer) TestServer() {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		workerAddr1  = "127.0.0.1:8262"
		keepAliveTTL = int64(1)
	)
	etcdDir := t.T().TempDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	t.Require().NoError(err)
	cfg := NewConfig()
	t.Require().NoError(cfg.Parse([]string{"-config=./dm-worker.toml"}))
	cfg.Join = masterAddr
	cfg.KeepAliveTTL = keepAliveTTL
	cfg.RelayKeepAliveTTL = keepAliveTTL

	NewRelayHolder = NewDummyRelayHolder
	NewSubTask = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client, worker string) *SubTask {
		cfg.UseRelay = false
		return NewRealSubTask(cfg, etcdClient, worker)
	}
	createUnits = func(cfg *config.SubTaskConfig, etcdClient *clientv3.Client, worker string, relay relay.Process) []unit.Unit {
		mockDumper := NewMockUnit(pb.UnitType_Dump)
		mockLoader := NewMockUnit(pb.UnitType_Load)
		mockSync := NewMockUnit(pb.UnitType_Sync)
		return []unit.Unit{mockDumper, mockLoader, mockSync}
	}
	defer func() {
		NewRelayHolder = NewRealRelayHolder
		NewSubTask = NewRealSubTask
		createUnits = createRealUnits
	}()

	s := NewServer(cfg)
	defer s.Close()
	go func() {
		err1 := s.Start()
		t.Require().NoError(err1)
	}()

	t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return !s.closed.Load()
	}))
	dir := t.T().TempDir()

	t.testOperateSourceBoundWithoutConfigInEtcd(s)

	t.testOperateWorker(s, dir, true)

	// check worker would retry connecting master rather than stop worker directly.
	ETCD = t.testRetryConnectMaster(s, ETCD, etcdDir, masterAddr)

	// resume contact with ETCD and start worker again
	t.testOperateWorker(s, dir, true)

	// test condition hub
	t.testConidtionHub(s)

	t.testHTTPInterface("status")
	t.testHTTPInterface("metrics")

	// create client
	cli := t.createClient(workerAddr1)

	// start task
	subtaskCfg := config.SubTaskConfig{}
	err = subtaskCfg.Decode(config.SampleSubtaskConfig, true)
	t.Require().NoError(err)
	subtaskCfg.MydumperPath = mydumperPath

	sourceCfg := loadSourceConfigWithoutPassword(t.T())
	_, err = ha.PutSubTaskCfgStage(s.etcdClient, []config.SubTaskConfig{subtaskCfg}, []ha.Stage{ha.NewSubTaskStage(pb.Stage_Running, sourceCfg.SourceID, subtaskCfg.Name)}, nil)
	t.Require().NoError(err)

	t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return checkSubTaskStatus(cli, pb.Stage_Running)
	}))

	t.testSubTaskRecover(s, dir)

	// pause relay
	_, err = ha.PutRelayStage(s.etcdClient, ha.NewRelayStage(pb.Stage_Paused, sourceCfg.SourceID))
	t.Require().NoError(err)
	t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return checkRelayStatus(cli, pb.Stage_Paused)
	}))
	// resume relay
	_, err = ha.PutRelayStage(s.etcdClient, ha.NewRelayStage(pb.Stage_Running, sourceCfg.SourceID))
	t.Require().NoError(err)
	t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return checkRelayStatus(cli, pb.Stage_Running)
	}))
	// pause task
	_, err = ha.PutSubTaskStage(s.etcdClient, ha.NewSubTaskStage(pb.Stage_Paused, sourceCfg.SourceID, subtaskCfg.Name))
	t.Require().NoError(err)
	t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return checkSubTaskStatus(cli, pb.Stage_Paused)
	}))

	// test refresh source cfg
	sourceCfg.MetaDir = "new meta"
	_, err = ha.PutSourceCfg(s.etcdClient, sourceCfg)
	t.Require().NoError(err)
	t.Require().NoError(s.worker.refreshSourceCfg())
	t.Require().Equal(sourceCfg.MetaDir, s.worker.cfg.MetaDir)

	// check update subtask cfg failed
	tomlStr, tomlErr := subtaskCfg.Toml()
	t.Require().NoError(tomlErr)
	ctx := context.Background()
	checkReq := &pb.CheckSubtasksCanUpdateRequest{SubtaskCfgTomlString: tomlStr}
	checkResp, checkErr := s.CheckSubtasksCanUpdate(ctx, checkReq)
	t.Require().NoError(checkErr)
	t.Require().False(checkResp.Success)

	// test refresh subtask cfg
	subtaskCfg.SyncerConfig.Batch = 111
	_, err = ha.PutSubTaskCfgStage(s.etcdClient, []config.SubTaskConfig{subtaskCfg}, []ha.Stage{}, []ha.Stage{})
	t.Require().NoError(err)
	subTask := s.worker.subTaskHolder.findSubTask(subtaskCfg.Name)
	subTask.setCurrUnit(subTask.units[2]) // set to syncer unit
	t.Require().NoError(s.worker.tryRefreshSubTaskAndSourceConfig(subTask))
	subtaskCfgInWorker := s.worker.subTaskHolder.findSubTask(subtaskCfg.Name)
	t.Require().Equal(subtaskCfg.SyncerConfig.Batch, subtaskCfgInWorker.cfg.SyncerConfig.Batch)

	// resume task
	_, err = ha.PutSubTaskStage(s.etcdClient, ha.NewSubTaskStage(pb.Stage_Running, sourceCfg.SourceID, subtaskCfg.Name))
	t.Require().NoError(err)
	t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return checkSubTaskStatus(cli, pb.Stage_Running)
	}))

	// stop task
	_, err = ha.DeleteSubTaskStage(s.etcdClient, ha.NewSubTaskStage(pb.Stage_Stopped, sourceCfg.SourceID, subtaskCfg.Name))
	t.Require().NoError(err)
	t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s.getSourceWorker(true).subTaskHolder.findSubTask(subtaskCfg.Name) == nil
	}))

	dupServer := NewServer(cfg)
	err = dupServer.Start()
	t.Require().True(terror.ErrWorkerStartService.Equal(err))
	t.Require().Regexp(".*bind: address already in use.*", err.Error())

	t.testStopWorkerWhenLostConnect(s, ETCD)
	s.Close()

	t.Require().True(utils.WaitSomething(30, 10*time.Millisecond, func() bool {
		return s.closed.Load()
	}))

	// test source worker, just make sure testing sort
	t.testSourceWorker()
}

func (t *testServer) TestHandleSourceBoundAfterError() {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
	)
	// start etcd server
	etcdDir := t.T().TempDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	t.Require().NoError(err)
	defer ETCD.Close()
	cfg := NewConfig()
	t.Require().NoError(cfg.Parse([]string{"-config=./dm-worker.toml"}))
	cfg.Join = masterAddr
	cfg.KeepAliveTTL = keepAliveTTL

	// new etcd client
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:            GetJoinURLs(cfg.Join),
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
	})
	t.Require().NoError(err)

	// watch worker event(oneline or offline)
	var (
		wg       sync.WaitGroup
		startRev int64 = 1
	)
	workerEvCh := make(chan ha.WorkerEvent, 10)
	workerErrCh := make(chan error, 10)
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer func() {
			close(workerEvCh)
			close(workerErrCh)
			wg.Done()
		}()
		ha.WatchWorkerEvent(ctx, etcdCli, startRev, workerEvCh, workerErrCh)
	}()

	// start worker server
	s := NewServer(cfg)
	defer s.Close()
	go func() {
		err1 := s.Start()
		t.Require().NoError(err1)
	}()
	t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return !s.closed.Load()
	}))

	// check if the worker is online
	t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		select {
		case ev := <-workerEvCh:
			if !ev.IsDeleted {
				return true
			}
		default:
		}
		return false
	}))

	// enable failpoint
	t.Require().NoError(failpoint.Enable("github.com/pingcap/tiflow/dm/pkg/ha/FailToGetSourceCfg", `return(true)`))
	sourceCfg := loadSourceConfigWithoutPassword(t.T())
	sourceCfg.EnableRelay = false
	_, err = ha.PutSourceCfg(etcdCli, sourceCfg)
	t.Require().NoError(err)
	sourceBound := ha.NewSourceBound(sourceCfg.SourceID, s.cfg.Name)
	_, err = ha.PutSourceBound(etcdCli, sourceBound)
	t.Require().NoError(err)

	// do check until worker offline
	t.Require().True(utils.WaitSomething(50, 100*time.Millisecond, func() bool {
		select {
		case ev := <-workerEvCh:
			if ev.IsDeleted {
				return true
			}
		default:
		}
		return false
	}))

	// check if the worker is online
	t.Require().True(utils.WaitSomething(5, time.Duration(s.cfg.KeepAliveTTL)*time.Second, func() bool {
		select {
		case ev := <-workerEvCh:
			if !ev.IsDeleted {
				return true
			}
		default:
		}
		return false
	}))

	// stop watching and disable failpoint
	cancel()
	wg.Wait()
	t.Require().NoError(failpoint.Disable("github.com/pingcap/tiflow/dm/pkg/ha/FailToGetSourceCfg"))

	_, err = ha.PutSourceBound(etcdCli, sourceBound)
	t.Require().NoError(err)
	_, err = ha.PutSourceCfg(etcdCli, sourceCfg)
	t.Require().NoError(err)
	t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s.getSourceWorker(true) != nil
	}))

	_, err = ha.DeleteSourceBound(etcdCli, s.cfg.Name)
	t.Require().NoError(err)
	t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s.getSourceWorker(true) == nil
	}))
}

func (t *testServer) TestServerQueryValidator() {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
	)
	etcdDir := t.T().TempDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	t.Require().NoError(err)
	defer ETCD.Close()
	cfg := NewConfig()
	t.Require().NoError(cfg.Parse([]string{"-config=./dm-worker.toml"}))
	cfg.Join = masterAddr
	cfg.KeepAliveTTL = keepAliveTTL
	cfg.RelayKeepAliveTTL = keepAliveTTL

	s := NewServer(cfg)
	resp, err := s.GetWorkerValidatorStatus(context.Background(), &pb.GetValidationStatusRequest{})
	t.Require().NoError(err)
	t.Require().False(resp.Result)
	t.Require().Regexp(".*no mysql source is being handled in the worker.*", resp.Msg)
}

func (t *testServer) TestServerQueryValidatorError() {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
	)
	etcdDir := t.T().TempDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	t.Require().NoError(err)
	defer ETCD.Close()
	cfg := NewConfig()
	t.Require().NoError(cfg.Parse([]string{"-config=./dm-worker.toml"}))
	cfg.Join = masterAddr
	cfg.KeepAliveTTL = keepAliveTTL
	cfg.RelayKeepAliveTTL = keepAliveTTL

	s := NewServer(cfg)
	resp, err := s.GetValidatorError(context.Background(), &pb.GetValidationErrorRequest{})
	t.Require().NoError(err)
	t.Require().False(resp.Result)
	t.Require().Regexp(".*no mysql source is being handled in the worker.*", resp.Msg)
}

func (t *testServer) TestServerOperateValidatorError() {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
	)
	etcdDir := t.T().TempDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	t.Require().NoError(err)
	defer ETCD.Close()
	cfg := NewConfig()
	t.Require().NoError(cfg.Parse([]string{"-config=./dm-worker.toml"}))
	cfg.Join = masterAddr
	cfg.KeepAliveTTL = keepAliveTTL
	cfg.RelayKeepAliveTTL = keepAliveTTL

	s := NewServer(cfg)
	resp, err := s.OperateValidatorError(context.Background(), &pb.OperateValidationErrorRequest{})
	t.Require().NoError(err)
	t.Require().False(resp.Result)
	t.Require().Regexp(".*no mysql source is being handled in the worker.*", resp.Msg)
}

func (t *testServer) TestWatchSourceBoundEtcdCompact() {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
		startRev     = int64(1)
	)
	etcdDir := t.T().TempDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	t.Require().NoError(err)
	defer ETCD.Close()
	cfg := NewConfig()
	t.Require().NoError(cfg.Parse([]string{"-config=./dm-worker.toml"}))
	cfg.Join = masterAddr
	cfg.KeepAliveTTL = keepAliveTTL
	cfg.RelayKeepAliveTTL = keepAliveTTL

	s := NewServer(cfg)
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:            GetJoinURLs(s.cfg.Join),
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
	})
	s.etcdClient = etcdCli
	s.closed.Store(false)
	t.Require().NoError(err)
	sourceCfg := loadSourceConfigWithoutPassword(t.T())
	sourceCfg.EnableRelay = false

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// step 1: Put a source config and source bound to this worker, then delete it
	_, err = ha.PutSourceCfg(etcdCli, sourceCfg)
	t.Require().NoError(err)
	sourceBound := ha.NewSourceBound(sourceCfg.SourceID, cfg.Name)
	_, err = ha.PutSourceBound(etcdCli, sourceBound)
	t.Require().NoError(err)
	rev, err := ha.DeleteSourceBound(etcdCli, cfg.Name)
	t.Require().NoError(err)
	// step 2: start source at this worker
	w, err := s.getOrStartWorker(sourceCfg, true)
	t.Require().NoError(err)
	t.Require().NoError(w.EnableHandleSubtasks())
	// step 3: trigger etcd compaction and check whether we can receive it through watcher
	_, err = etcdCli.Compact(ctx, rev)
	t.Require().NoError(err)
	sourceBoundCh := make(chan ha.SourceBound, 10)
	sourceBoundErrCh := make(chan error, 10)
	ha.WatchSourceBound(ctx, etcdCli, cfg.Name, startRev, sourceBoundCh, sourceBoundErrCh)
	select {
	case err = <-sourceBoundErrCh:
		t.Require().Equal(etcdErrCompacted, errors.Cause(err))
	case <-time.After(300 * time.Millisecond):
		t.T().Fatal("fail to get etcd error compacted")
	}
	// step 4: watch source bound from startRev
	var wg sync.WaitGroup
	ctx1, cancel1 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		t.Require().NoError(s.observeSourceBound(ctx1, startRev))
	}()
	// step 4.1: should stop the running worker, source bound has been deleted, should stop this worker
	t.Require().True(utils.WaitSomething(20, 100*time.Millisecond, func() bool {
		return s.getSourceWorker(true) == nil
	}))
	// step 4.2: put a new source bound, source should be started
	_, err = ha.PutSourceBound(etcdCli, sourceBound)
	t.Require().NoError(err)
	t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s.getSourceWorker(true) != nil
	}))
	cfg2 := s.getSourceWorker(true).cfg
	t.Require().Equal(sourceCfg, cfg2)
	cancel1()
	wg.Wait()
	t.Require().NoError(s.stopSourceWorker(sourceCfg.SourceID, true, true))
	// step 5: start observeSourceBound from compacted revision again, should start worker
	ctx2, cancel2 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		t.Require().NoError(s.observeSourceBound(ctx2, startRev))
	}()
	t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s.getSourceWorker(true) != nil
	}))
	cfg2 = s.getSourceWorker(true).cfg
	t.Require().Equal(sourceCfg, cfg2)
	cancel2()
	wg.Wait()
}

func (t *testServer) testHTTPInterface(uri string) {
	// nolint:noctx
	resp, err := http.Get("http://127.0.0.1:8262/" + uri)
	t.Require().NoError(err)
	defer resp.Body.Close()
	t.Require().Equal(200, resp.StatusCode)
	_, err = io.ReadAll(resp.Body)
	t.Require().NoError(err)
}

func (t *testServer) createClient(addr string) pb.WorkerClient {
	//nolint:staticcheck
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBackoffMaxDelay(3*time.Second))
	t.Require().NoError(err)
	return pb.NewWorkerClient(conn)
}

func (t *testServer) testOperateSourceBoundWithoutConfigInEtcd(s *Server) {
	err := s.operateSourceBound(ha.NewSourceBound("sourceWithoutConfigInEtcd", s.cfg.Name))
	t.Require().True(terror.ErrWorkerFailToGetSourceConfigFromEtcd.Equal(err))
}

func (t *testServer) testOperateWorker(s *Server, dir string, start bool) {
	// load sourceCfg
	sourceCfg := loadSourceConfigWithoutPassword(t.T())
	sourceCfg.EnableRelay = true
	sourceCfg.RelayDir = dir
	sourceCfg.MetaDir = t.T().TempDir()

	if start {
		// put mysql config into relative etcd key adapter to trigger operation event
		_, err := ha.PutSourceCfg(s.etcdClient, sourceCfg)
		t.Require().NoError(err)
		_, err = ha.PutRelayStageRelayConfigSourceBound(s.etcdClient, ha.NewRelayStage(pb.Stage_Running, sourceCfg.SourceID),
			ha.NewSourceBound(sourceCfg.SourceID, s.cfg.Name))
		t.Require().NoError(err)
		// worker should be started and without error
		t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
			w := s.getSourceWorker(true)
			return w != nil && !w.closed.Load()
		}))
		t.Require().Nil(s.getSourceStatus(true).Result)
	} else {
		// worker should be started before stopped
		w := s.getSourceWorker(true)
		t.Require().NotNil(w)
		t.Require().False(w.closed.Load())
		_, err := ha.DeleteRelayConfig(s.etcdClient, w.name)
		t.Require().NoError(err)
		_, err = ha.DeleteSourceCfgRelayStageSourceBound(s.etcdClient, sourceCfg.SourceID, s.cfg.Name)
		t.Require().NoError(err)
		// worker should be closed and without error
		t.Require().True(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
			currentWorker := s.getSourceWorker(true)
			return currentWorker == nil && w.closed.Load()
		}))
		t.Require().Nil(s.getSourceStatus(true).Result)
	}
}

func (t *testServer) testRetryConnectMaster(s *Server, etcd *embed.Etcd, dir string, hostName string) *embed.Etcd {
	etcd.Close()
	time.Sleep(6 * time.Second)
	// When worker server fail to keepalive with etcd, server should close its worker
	t.Require().Nil(s.getSourceWorker(true))
	t.Require().Nil(s.getSourceStatus(true).Result)
	ETCD, err := createMockETCD(dir, "http://"+hostName)
	t.Require().NoError(err)
	time.Sleep(3 * time.Second)
	return ETCD
}

func (t *testServer) testSubTaskRecover(s *Server, dir string) {
	workerCli := t.createClient("127.0.0.1:8262")
	t.testOperateWorker(s, dir, false)

	status, err := workerCli.QueryStatus(context.Background(), &pb.QueryStatusRequest{Name: "sub-task-name"})
	t.Require().NoError(err)
	t.Require().False(status.Result)
	t.Require().Equal(terror.ErrWorkerNoStart.Error(), status.Msg)

	t.testOperateWorker(s, dir, true)

	// because we split starting worker and enabling handling subtasks into two parts, a query-status may occur between
	// them, thus get a result of no subtask running
	utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		status, err = workerCli.QueryStatus(context.Background(), &pb.QueryStatusRequest{Name: "sub-task-name"})
		if err != nil {
			return false
		}
		if status.Result == false {
			return false
		}
		if len(status.SubTaskStatus) == 0 || status.SubTaskStatus[0].Stage != pb.Stage_Running {
			return false
		}
		return true
	})

	status, err = workerCli.QueryStatus(context.Background(), &pb.QueryStatusRequest{Name: "sub-task-name"})
	t.Require().NoError(err)
	t.Require().True(status.Result)
	t.Require().Len(status.SubTaskStatus, 1)
	t.Require().Equal(pb.Stage_Running, status.SubTaskStatus[0].Stage)
}

func (t *testServer) testStopWorkerWhenLostConnect(s *Server, etcd *embed.Etcd) {
	etcd.Close()
	t.Require().True(utils.WaitSomething(int(defaultKeepAliveTTL+3), time.Second, func() bool {
		return s.getSourceWorker(true) == nil
	}))
	t.Require().Nil(s.getSourceWorker(true))
}

func (t *testServer) TestGetMinLocInAllSubTasks() {
	subTaskCfg := map[string]config.SubTaskConfig{
		"test2": {Name: "test2"},
		"test3": {Name: "test3"},
		"test1": {Name: "test1"},
	}
	minLoc, err := getMinLocInAllSubTasks(context.Background(), subTaskCfg)
	t.Require().NoError(err)
	t.Require().Equal("mysql-binlog.00001", minLoc.Position.Name)
	t.Require().Equal(uint32(12), minLoc.Position.Pos)

	for k, cfg := range subTaskCfg {
		cfg.EnableGTID = true
		subTaskCfg[k] = cfg
	}

	minLoc, err = getMinLocInAllSubTasks(context.Background(), subTaskCfg)
	t.Require().NoError(err)
	t.Require().Equal("mysql-binlog.00001", minLoc.Position.Name)
	t.Require().Equal(uint32(123), minLoc.Position.Pos)
}

func getFakeLocForSubTask(ctx context.Context, subTaskCfg config.SubTaskConfig) (minLoc *binlog.Location, err error) {
	gset1, _ := gtid.ParserGTID(mysql.MySQLFlavor, "ba8f633f-1f15-11eb-b1c7-0242ac110001:1-30")
	gset2, _ := gtid.ParserGTID(mysql.MySQLFlavor, "ba8f633f-1f15-11eb-b1c7-0242ac110001:1-50")
	gset3, _ := gtid.ParserGTID(mysql.MySQLFlavor, "ba8f633f-1f15-11eb-b1c7-0242ac110001:1-50,ba8f633f-1f15-11eb-b1c7-0242ac110002:1")
	loc1 := binlog.NewLocation(
		mysql.Position{
			Name: "mysql-binlog.00001",
			Pos:  123,
		},
		gset1,
	)
	loc2 := binlog.NewLocation(
		mysql.Position{
			Name: "mysql-binlog.00001",
			Pos:  12,
		},
		gset2,
	)
	loc3 := binlog.NewLocation(
		mysql.Position{
			Name: "mysql-binlog.00003",
		},
		gset3,
	)

	switch subTaskCfg.Name {
	case "test1":
		return &loc1, nil
	case "test2":
		return &loc2, nil
	case "test3":
		return &loc3, nil
	default:
		return nil, nil
	}
}

func checkSubTaskStatus(cli pb.WorkerClient, expect pb.Stage) bool {
	status, err := cli.QueryStatus(context.Background(), &pb.QueryStatusRequest{Name: "sub-task-name"})
	if err != nil {
		return false
	}
	if status.Result == false {
		return false
	}
	return len(status.SubTaskStatus) > 0 && status.SubTaskStatus[0].Stage == expect
}

func checkRelayStatus(cli pb.WorkerClient, expect pb.Stage) bool {
	status, err := cli.QueryStatus(context.Background(), &pb.QueryStatusRequest{Name: "sub-task-name"})
	if err != nil {
		return false
	}
	if status.Result == false {
		return false
	}
	return status.SourceStatus.RelayStatus.Stage == expect
}

func loadSourceConfigWithoutPassword(t require.TestingT) *config.SourceConfig {
	sourceCfg, err := config.SourceCfgFromYamlAndVerify(config.SampleSourceConfig)
	require.NoError(t, err)
	sourceCfg.From.Password = "" // no password set
	return sourceCfg
}

func (t *testServer) TestServerDataRace() {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
	)
	etcdDir := t.T().TempDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	t.Require().NoError(err)
	defer ETCD.Close()
	cfg := NewConfig()
	t.Require().NoError(cfg.Parse([]string{"-config=./dm-worker.toml"}))
	cfg.Join = masterAddr
	cfg.KeepAliveTTL = keepAliveTTL
	cfg.RelayKeepAliveTTL = keepAliveTTL

	s := NewServer(cfg)
	defer s.Close()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			err1 := s.Start()
			t.Require().True(err1 == nil || err1 == terror.ErrWorkerServerClosed)
		}()
		go func() {
			defer wg.Done()
			s.Close()
		}()
		wg.Wait()
	}
}

func loadSourceConfigWithoutPassword2(t *testing.T) *config.SourceConfig {
	t.Helper()

	sourceCfg, err := config.SourceCfgFromYamlAndVerify(config.SampleSourceConfig)
	require.NoError(t, err)
	sourceCfg.From.Password = "" // no password set
	return sourceCfg
}
