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

package servermaster

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/pingcap/log"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/pkg/election"
	"github.com/pingcap/tiflow/engine/pkg/rpcutil"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func generateNodeID(name string) string {
	val := rand.Uint32()
	id := fmt.Sprintf("%s-%08x", name, val)
	return id
}

// multiClient is an interface that implements all the Client interfaces
// for the individual services running on the server masters.
type multiClient interface {
	pb.DiscoveryClient
	pb.ResourceManagerClient
	pb.TaskSchedulerClient
	pb.JobManagerClient
}

type multiClientImpl struct {
	pb.DiscoveryClient
	pb.ResourceManagerClient
	pb.TaskSchedulerClient
	pb.JobManagerClient
}

func newMultiClient(conn *grpc.ClientConn) multiClient {
	return &multiClientImpl{
		DiscoveryClient:       pb.NewDiscoveryClient(conn),
		ResourceManagerClient: pb.NewResourceManagerClient(conn),
		TaskSchedulerClient:   pb.NewTaskSchedulerClient(conn),
		JobManagerClient:      pb.NewJobManagerClient(conn),
	}
}

var leaderOnlyMethods = map[string]struct{}{
	"ListExecutors":    {},
	"RegisterExecutor": {},
	"Heartbeat":        {},
	"CreateJob":        {},
	"GetJob":           {},
	"ListJobs":         {},
	"CancelJob":        {},
	"DeleteJob":        {},
	"ScheduleTask":     {},
}

var _ rpcutil.ForwardChecker[multiClient] = &forwardChecker{}

type forwardChecker struct {
	elector election.Elector

	rwm       sync.RWMutex
	conn      *grpc.ClientConn
	leaderCli multiClient
}

func newForwardChecker(elector election.Elector) *forwardChecker {
	return &forwardChecker{
		elector: elector,
	}
}

func (f *forwardChecker) LeaderOnly(method string) bool {
	_, ok := leaderOnlyMethods[method]
	return ok
}

func (f *forwardChecker) IsLeader() bool {
	return f.elector.IsLeader()
}

func (f *forwardChecker) LeaderClient() (multiClient, error) {
	leader, ok := f.elector.GetLeader()
	if !ok {
		return nil, errors.ErrMasterNoLeader.GenWithStackByArgs()
	}
	return f.getOrCreateLeaderClient(leader.Address)
}

func (f *forwardChecker) getOrCreateLeaderClient(leaderAddr string) (multiClient, error) {
	f.rwm.RLock()
	if f.conn != nil && f.conn.Target() == leaderAddr {
		f.rwm.RUnlock()
		return f.leaderCli, nil
	}
	f.rwm.RUnlock()

	f.rwm.Lock()
	defer f.rwm.Unlock()

	if f.conn != nil {
		if f.conn.Target() == leaderAddr {
			return f.leaderCli, nil
		}
		if err := f.conn.Close(); err != nil {
			log.Warn("failed to close grpc connection", zap.Error(err))
		}
		f.conn = nil
	}

	conn, err := grpc.Dial(leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, errors.Cause(err)
	}
	f.conn = conn
	f.leaderCli = newMultiClient(conn)
	return f.leaderCli, nil
}

func (f *forwardChecker) Close() error {
	f.rwm.Lock()
	defer f.rwm.Unlock()

	var err error
	if f.conn != nil {
		err = f.conn.Close()
		f.conn = nil
	}
	f.leaderCli = nil
	return err
}

// ensure featureDegrader implements rpcutil.FeatureChecker
var _ rpcutil.FeatureChecker = &featureDegrader{}

// featureDegrader is used to record whether a feature is available or degradation
// in server master.
type featureDegrader struct {
	executorManager     atomic.Bool
	masterWorkerManager atomic.Bool
}

func newFeatureDegrader() *featureDegrader {
	fd := &featureDegrader{}
	fd.reset()
	return fd
}

func (d *featureDegrader) updateExecutorManager(val bool) {
	d.executorManager.Store(val)
}

func (d *featureDegrader) updateMasterWorkerManager(val bool) {
	d.masterWorkerManager.Store(val)
}

func (d *featureDegrader) reset() {
	d.executorManager.Store(false)
	d.masterWorkerManager.Store(false)
}

// Available implements rpcutil.FeatureChecker
func (d *featureDegrader) Available(method string) bool {
	switch method {
	case "ListExecutors", "RegisterExecutor", "Heartbeat":
		return d.executorManager.Load()
	case "CreateJob", "GetJob", "ListJobs", "CancelJob", "DeleteJob",
		"ScheduleTask":
		return d.masterWorkerManager.Load()
	}
	return true
}
