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
	. "github.com/pingcap/check"
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

func TestServer(t *testing.T) {
	TestingT(t)
}

type testServer struct{}

var _ = Suite(&testServer{})

func (t *testServer) SetUpSuite(c *C) {
	err := log.InitLogger(&log.Config{})
	c.Assert(err, IsNil)
	getMinLocForSubTaskFunc = getFakeLocForSubTask
}

func (t *testServer) TearDownSuite(c *C) {
	getMinLocForSubTaskFunc = getMinLocForSubTask
}

func createMockETCD(dir string, host string) (*embed.Etcd, error) {
	cfg := embed.NewConfig()
	cfg.Dir = dir
	lcurl, _ := url.Parse(host)
	cfg.LCUrls = []url.URL{*lcurl}
	cfg.ACUrls = []url.URL{*lcurl}
	lpurl, _ := url.Parse(tempurl.Alloc())
	cfg.LPUrls = []url.URL{*lpurl}
	cfg.APUrls = []url.URL{*lpurl}
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

func (t *testServer) TestServer(c *C) {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		workerAddr1  = "127.0.0.1:8262"
		keepAliveTTL = int64(1)
	)
	etcdDir := c.MkDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	c.Assert(err, IsNil)
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)
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
		c.Assert(err1, IsNil)
	}()

	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return !s.closed.Load()
	}), IsTrue)
	dir := c.MkDir()

	t.testOperateSourceBoundWithoutConfigInEtcd(c, s)

	t.testOperateWorker(c, s, dir, true)

	// check worker would retry connecting master rather than stop worker directly.
	ETCD = t.testRetryConnectMaster(c, s, ETCD, etcdDir, masterAddr)

	// resume contact with ETCD and start worker again
	t.testOperateWorker(c, s, dir, true)

	// test condition hub
	t.testConidtionHub(c, s)

	t.testHTTPInterface(c, "status")
	t.testHTTPInterface(c, "metrics")

	// create client
	cli := t.createClient(c, workerAddr1)

	// start task
	subtaskCfg := config.SubTaskConfig{}
	err = subtaskCfg.Decode(config.SampleSubtaskConfig, true)
	c.Assert(err, IsNil)
	subtaskCfg.MydumperPath = mydumperPath

	sourceCfg := loadSourceConfigWithoutPassword(c)
	_, err = ha.PutSubTaskCfgStage(s.etcdClient, []config.SubTaskConfig{subtaskCfg}, []ha.Stage{ha.NewSubTaskStage(pb.Stage_Running, sourceCfg.SourceID, subtaskCfg.Name)}, nil)
	c.Assert(err, IsNil)

	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return checkSubTaskStatus(cli, pb.Stage_Running)
	}), IsTrue)

	t.testSubTaskRecover(c, s, dir)

	// pause relay
	_, err = ha.PutRelayStage(s.etcdClient, ha.NewRelayStage(pb.Stage_Paused, sourceCfg.SourceID))
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return checkRelayStatus(cli, pb.Stage_Paused)
	}), IsTrue)
	// resume relay
	_, err = ha.PutRelayStage(s.etcdClient, ha.NewRelayStage(pb.Stage_Running, sourceCfg.SourceID))
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return checkRelayStatus(cli, pb.Stage_Running)
	}), IsTrue)
	// pause task
	_, err = ha.PutSubTaskStage(s.etcdClient, ha.NewSubTaskStage(pb.Stage_Paused, sourceCfg.SourceID, subtaskCfg.Name))
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return checkSubTaskStatus(cli, pb.Stage_Paused)
	}), IsTrue)

	// test refresh source cfg
	sourceCfg.MetaDir = "new meta"
	_, err = ha.PutSourceCfg(s.etcdClient, sourceCfg)
	c.Assert(err, IsNil)
	c.Assert(s.worker.refreshSourceCfg(), IsNil)
	c.Assert(s.worker.cfg.MetaDir, Equals, sourceCfg.MetaDir)

	// check update subtask cfg failed
	tomlStr, tomlErr := subtaskCfg.Toml()
	c.Assert(tomlErr, IsNil)
	ctx := context.Background()
	checkReq := &pb.CheckSubtasksCanUpdateRequest{SubtaskCfgTomlString: tomlStr}
	checkResp, checkErr := s.CheckSubtasksCanUpdate(ctx, checkReq)
	c.Assert(checkErr, IsNil)
	c.Assert(checkResp.Success, IsFalse)

	// test refresh subtask cfg
	subtaskCfg.SyncerConfig.Batch = 111
	_, err = ha.PutSubTaskCfgStage(s.etcdClient, []config.SubTaskConfig{subtaskCfg}, []ha.Stage{}, []ha.Stage{})
	c.Assert(err, IsNil)
	subTask := s.worker.subTaskHolder.findSubTask(subtaskCfg.Name)
	subTask.setCurrUnit(subTask.units[2]) // set to syncer unit
	c.Assert(s.worker.tryRefreshSubTaskAndSourceConfig(subTask), IsNil)
	subtaskCfgInWorker := s.worker.subTaskHolder.findSubTask(subtaskCfg.Name)
	c.Assert(subtaskCfgInWorker.cfg.SyncerConfig.Batch, Equals, subtaskCfg.SyncerConfig.Batch)

	// resume task
	_, err = ha.PutSubTaskStage(s.etcdClient, ha.NewSubTaskStage(pb.Stage_Running, sourceCfg.SourceID, subtaskCfg.Name))
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return checkSubTaskStatus(cli, pb.Stage_Running)
	}), IsTrue)

	// stop task
	_, err = ha.DeleteSubTaskStage(s.etcdClient, ha.NewSubTaskStage(pb.Stage_Stopped, sourceCfg.SourceID, subtaskCfg.Name))
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s.getSourceWorker(true).subTaskHolder.findSubTask(subtaskCfg.Name) == nil
	}), IsTrue)

	dupServer := NewServer(cfg)
	err = dupServer.Start()
	c.Assert(terror.ErrWorkerStartService.Equal(err), IsTrue)
	c.Assert(err.Error(), Matches, ".*bind: address already in use.*")

	t.testStopWorkerWhenLostConnect(c, s, ETCD)
	s.Close()

	c.Assert(utils.WaitSomething(30, 10*time.Millisecond, func() bool {
		return s.closed.Load()
	}), IsTrue)

	// test source worker, just make sure testing sort
	t.testSourceWorker(c)
}

func (t *testServer) TestHandleSourceBoundAfterError(c *C) {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
	)
	// start etcd server
	etcdDir := c.MkDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	c.Assert(err, IsNil)
	defer ETCD.Close()
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)
	cfg.Join = masterAddr
	cfg.KeepAliveTTL = keepAliveTTL

	// new etcd client
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:            GetJoinURLs(cfg.Join),
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
	})
	c.Assert(err, IsNil)

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
		c.Assert(err1, IsNil)
	}()
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return !s.closed.Load()
	}), IsTrue)

	// check if the worker is online
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		select {
		case ev := <-workerEvCh:
			if !ev.IsDeleted {
				return true
			}
		default:
		}
		return false
	}), IsTrue)

	// enable failpoint
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/pkg/ha/FailToGetSourceCfg", `return(true)`), IsNil)
	sourceCfg := loadSourceConfigWithoutPassword(c)
	sourceCfg.EnableRelay = false
	_, err = ha.PutSourceCfg(etcdCli, sourceCfg)
	c.Assert(err, IsNil)
	sourceBound := ha.NewSourceBound(sourceCfg.SourceID, s.cfg.Name)
	_, err = ha.PutSourceBound(etcdCli, sourceBound)
	c.Assert(err, IsNil)

	// do check until worker offline
	c.Assert(utils.WaitSomething(50, 100*time.Millisecond, func() bool {
		select {
		case ev := <-workerEvCh:
			if ev.IsDeleted {
				return true
			}
		default:
		}
		return false
	}), IsTrue)

	// check if the worker is online
	c.Assert(utils.WaitSomething(5, time.Duration(s.cfg.KeepAliveTTL)*time.Second, func() bool {
		select {
		case ev := <-workerEvCh:
			if !ev.IsDeleted {
				return true
			}
		default:
		}
		return false
	}), IsTrue)

	// stop watching and disable failpoint
	cancel()
	wg.Wait()
	c.Assert(failpoint.Disable("github.com/pingcap/tiflow/dm/pkg/ha/FailToGetSourceCfg"), IsNil)

	_, err = ha.PutSourceBound(etcdCli, sourceBound)
	c.Assert(err, IsNil)
	_, err = ha.PutSourceCfg(etcdCli, sourceCfg)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s.getSourceWorker(true) != nil
	}), IsTrue)

	_, err = ha.DeleteSourceBound(etcdCli, s.cfg.Name)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s.getSourceWorker(true) == nil
	}), IsTrue)
}

func (t *testServer) TestServerQueryValidator(c *C) {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
	)
	etcdDir := c.MkDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	c.Assert(err, IsNil)
	defer ETCD.Close()
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)
	cfg.Join = masterAddr
	cfg.KeepAliveTTL = keepAliveTTL
	cfg.RelayKeepAliveTTL = keepAliveTTL

	s := NewServer(cfg)
	resp, err := s.GetWorkerValidatorStatus(context.Background(), &pb.GetValidationStatusRequest{})
	c.Assert(err, IsNil)
	c.Assert(resp.Result, IsFalse)
	c.Assert(resp.Msg, Matches, ".*no mysql source is being handled in the worker.*")
}

func (t *testServer) TestServerQueryValidatorError(c *C) {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
	)
	etcdDir := c.MkDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	c.Assert(err, IsNil)
	defer ETCD.Close()
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)
	cfg.Join = masterAddr
	cfg.KeepAliveTTL = keepAliveTTL
	cfg.RelayKeepAliveTTL = keepAliveTTL

	s := NewServer(cfg)
	resp, err := s.GetValidatorError(context.Background(), &pb.GetValidationErrorRequest{})
	c.Assert(err, IsNil)
	c.Assert(resp.Result, IsFalse)
	c.Assert(resp.Msg, Matches, ".*no mysql source is being handled in the worker.*")
}

func (t *testServer) TestServerOperateValidatorError(c *C) {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
	)
	etcdDir := c.MkDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	c.Assert(err, IsNil)
	defer ETCD.Close()
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)
	cfg.Join = masterAddr
	cfg.KeepAliveTTL = keepAliveTTL
	cfg.RelayKeepAliveTTL = keepAliveTTL

	s := NewServer(cfg)
	resp, err := s.OperateValidatorError(context.Background(), &pb.OperateValidationErrorRequest{})
	c.Assert(err, IsNil)
	c.Assert(resp.Result, IsFalse)
	c.Assert(resp.Msg, Matches, ".*no mysql source is being handled in the worker.*")
}

func (t *testServer) TestWatchSourceBoundEtcdCompact(c *C) {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
		startRev     = int64(1)
	)
	etcdDir := c.MkDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	c.Assert(err, IsNil)
	defer ETCD.Close()
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)
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
	c.Assert(err, IsNil)
	sourceCfg := loadSourceConfigWithoutPassword(c)
	sourceCfg.EnableRelay = false

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// step 1: Put a source config and source bound to this worker, then delete it
	_, err = ha.PutSourceCfg(etcdCli, sourceCfg)
	c.Assert(err, IsNil)
	sourceBound := ha.NewSourceBound(sourceCfg.SourceID, cfg.Name)
	_, err = ha.PutSourceBound(etcdCli, sourceBound)
	c.Assert(err, IsNil)
	rev, err := ha.DeleteSourceBound(etcdCli, cfg.Name)
	c.Assert(err, IsNil)
	// step 2: start source at this worker
	w, err := s.getOrStartWorker(sourceCfg, true)
	c.Assert(err, IsNil)
	c.Assert(w.EnableHandleSubtasks(), IsNil)
	// step 3: trigger etcd compaction and check whether we can receive it through watcher
	_, err = etcdCli.Compact(ctx, rev)
	c.Assert(err, IsNil)
	sourceBoundCh := make(chan ha.SourceBound, 10)
	sourceBoundErrCh := make(chan error, 10)
	ha.WatchSourceBound(ctx, etcdCli, cfg.Name, startRev, sourceBoundCh, sourceBoundErrCh)
	select {
	case err = <-sourceBoundErrCh:
		c.Assert(errors.Cause(err), Equals, etcdErrCompacted)
	case <-time.After(300 * time.Millisecond):
		c.Fatal("fail to get etcd error compacted")
	}
	// step 4: watch source bound from startRev
	var wg sync.WaitGroup
	ctx1, cancel1 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(s.observeSourceBound(ctx1, startRev), IsNil)
	}()
	// step 4.1: should stop the running worker, source bound has been deleted, should stop this worker
	c.Assert(utils.WaitSomething(20, 100*time.Millisecond, func() bool {
		return s.getSourceWorker(true) == nil
	}), IsTrue)
	// step 4.2: put a new source bound, source should be started
	_, err = ha.PutSourceBound(etcdCli, sourceBound)
	c.Assert(err, IsNil)
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s.getSourceWorker(true) != nil
	}), IsTrue)
	cfg2 := s.getSourceWorker(true).cfg
	c.Assert(cfg2, DeepEquals, sourceCfg)
	cancel1()
	wg.Wait()
	c.Assert(s.stopSourceWorker(sourceCfg.SourceID, true, true), IsNil)
	// step 5: start observeSourceBound from compacted revision again, should start worker
	ctx2, cancel2 := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Assert(s.observeSourceBound(ctx2, startRev), IsNil)
	}()
	c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		return s.getSourceWorker(true) != nil
	}), IsTrue)
	cfg2 = s.getSourceWorker(true).cfg
	c.Assert(cfg2, DeepEquals, sourceCfg)
	cancel2()
	wg.Wait()
}

func (t *testServer) testHTTPInterface(c *C, uri string) {
	// nolint:noctx
	resp, err := http.Get("http://127.0.0.1:8262/" + uri)
	c.Assert(err, IsNil)
	defer resp.Body.Close()
	c.Assert(resp.StatusCode, Equals, 200)
	_, err = io.ReadAll(resp.Body)
	c.Assert(err, IsNil)
}

func (t *testServer) createClient(c *C, addr string) pb.WorkerClient {
	//nolint:staticcheck
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBackoffMaxDelay(3*time.Second))
	c.Assert(err, IsNil)
	return pb.NewWorkerClient(conn)
}

func (t *testServer) testOperateSourceBoundWithoutConfigInEtcd(c *C, s *Server) {
	err := s.operateSourceBound(ha.NewSourceBound("sourceWithoutConfigInEtcd", s.cfg.Name))
	c.Assert(terror.ErrWorkerFailToGetSourceConfigFromEtcd.Equal(err), IsTrue)
}

func (t *testServer) testOperateWorker(c *C, s *Server, dir string, start bool) {
	// load sourceCfg
	sourceCfg := loadSourceConfigWithoutPassword(c)
	sourceCfg.EnableRelay = true
	sourceCfg.RelayDir = dir
	sourceCfg.MetaDir = c.MkDir()

	if start {
		// put mysql config into relative etcd key adapter to trigger operation event
		_, err := ha.PutSourceCfg(s.etcdClient, sourceCfg)
		c.Assert(err, IsNil)
		_, err = ha.PutRelayStageRelayConfigSourceBound(s.etcdClient, ha.NewRelayStage(pb.Stage_Running, sourceCfg.SourceID),
			ha.NewSourceBound(sourceCfg.SourceID, s.cfg.Name))
		c.Assert(err, IsNil)
		// worker should be started and without error
		c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
			w := s.getSourceWorker(true)
			return w != nil && !w.closed.Load()
		}), IsTrue)
		c.Assert(s.getSourceStatus(true).Result, IsNil)
	} else {
		// worker should be started before stopped
		w := s.getSourceWorker(true)
		c.Assert(w, NotNil)
		c.Assert(w.closed.Load(), IsFalse)
		_, err := ha.DeleteRelayConfig(s.etcdClient, w.name)
		c.Assert(err, IsNil)
		_, err = ha.DeleteSourceCfgRelayStageSourceBound(s.etcdClient, sourceCfg.SourceID, s.cfg.Name)
		c.Assert(err, IsNil)
		// worker should be closed and without error
		c.Assert(utils.WaitSomething(30, 100*time.Millisecond, func() bool {
			currentWorker := s.getSourceWorker(true)
			return currentWorker == nil && w.closed.Load()
		}), IsTrue)
		c.Assert(s.getSourceStatus(true).Result, IsNil)
	}
}

func (t *testServer) testRetryConnectMaster(c *C, s *Server, etcd *embed.Etcd, dir string, hostName string) *embed.Etcd {
	etcd.Close()
	time.Sleep(6 * time.Second)
	// When worker server fail to keepalive with etcd, server should close its worker
	c.Assert(s.getSourceWorker(true), IsNil)
	c.Assert(s.getSourceStatus(true).Result, IsNil)
	ETCD, err := createMockETCD(dir, "http://"+hostName)
	c.Assert(err, IsNil)
	time.Sleep(3 * time.Second)
	return ETCD
}

func (t *testServer) testSubTaskRecover(c *C, s *Server, dir string) {
	workerCli := t.createClient(c, "127.0.0.1:8262")
	t.testOperateWorker(c, s, dir, false)

	status, err := workerCli.QueryStatus(context.Background(), &pb.QueryStatusRequest{Name: "sub-task-name"})
	c.Assert(err, IsNil)
	c.Assert(status.Result, IsFalse)
	c.Assert(status.Msg, Equals, terror.ErrWorkerNoStart.Error())

	t.testOperateWorker(c, s, dir, true)

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
	c.Assert(err, IsNil)
	c.Assert(status.Result, IsTrue)
	c.Assert(status.SubTaskStatus, HasLen, 1)
	c.Assert(status.SubTaskStatus[0].Stage, Equals, pb.Stage_Running)
}

func (t *testServer) testStopWorkerWhenLostConnect(c *C, s *Server, etcd *embed.Etcd) {
	etcd.Close()
	c.Assert(utils.WaitSomething(int(defaultKeepAliveTTL+3), time.Second, func() bool {
		return s.getSourceWorker(true) == nil
	}), IsTrue)
	c.Assert(s.getSourceWorker(true), IsNil)
}

func (t *testServer) TestGetMinLocInAllSubTasks(c *C) {
	subTaskCfg := map[string]config.SubTaskConfig{
		"test2": {Name: "test2"},
		"test3": {Name: "test3"},
		"test1": {Name: "test1"},
	}
	minLoc, err := getMinLocInAllSubTasks(context.Background(), subTaskCfg)
	c.Assert(err, IsNil)
	c.Assert(minLoc.Position.Name, Equals, "mysql-binlog.00001")
	c.Assert(minLoc.Position.Pos, Equals, uint32(12))

	for k, cfg := range subTaskCfg {
		cfg.EnableGTID = true
		subTaskCfg[k] = cfg
	}

	minLoc, err = getMinLocInAllSubTasks(context.Background(), subTaskCfg)
	c.Assert(err, IsNil)
	c.Assert(minLoc.Position.Name, Equals, "mysql-binlog.00001")
	c.Assert(minLoc.Position.Pos, Equals, uint32(123))
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

func loadSourceConfigWithoutPassword(c *C) *config.SourceConfig {
	sourceCfg, err := config.ParseYamlAndVerify(config.SampleSourceConfig)
	c.Assert(err, IsNil)
	sourceCfg.From.Password = "" // no password set
	return sourceCfg
}

func (t *testServer) TestServerDataRace(c *C) {
	var (
		masterAddr   = tempurl.Alloc()[len("http://"):]
		keepAliveTTL = int64(1)
	)
	etcdDir := c.MkDir()
	ETCD, err := createMockETCD(etcdDir, "http://"+masterAddr)
	c.Assert(err, IsNil)
	defer ETCD.Close()
	cfg := NewConfig()
	c.Assert(cfg.Parse([]string{"-config=./dm-worker.toml"}), IsNil)
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
			c.Assert(err1 == nil || err1 == terror.ErrWorkerServerClosed, IsTrue)
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

	sourceCfg, err := config.ParseYamlAndVerify(config.SampleSourceConfig)
	require.NoError(t, err)
	sourceCfg.From.Password = "" // no password set
	return sourceCfg
}
