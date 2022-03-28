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

package scheduler

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

func genSourceID(i int) string {
	return fmt.Sprintf("mysql-replica-%d", i)
}

func genSourceCfg(t *testing.T, i int) *config.SourceConfig {
	sourceCfg, err := config.ParseYamlAndVerify(config.SampleSourceConfig)
	require.NoError(t, err)
	sourceCfg.SourceID = genSourceID(i)
	return sourceCfg
}

func genWorkerName(i int) string {
	return fmt.Sprintf("dm-worker-%d", i)
}

func genWorkerAddr(i int) string {
	return fmt.Sprintf("127.0.0.1:%d", 31360+i)
}

func genTaskName(i int) string {
	return "task" + strconv.Itoa(i)
}

func genSourceBounds(lSource, rSource, worker int) []ha.SourceBound {
	bounds := make([]ha.SourceBound, rSource-lSource+1)
	for i := lSource; i <= rSource; i++ {
		bounds = append(bounds, ha.SourceBound{
			Source: genSourceID(i),
			Worker: genWorkerName(worker),
		})
	}
	return bounds
}

func TestBalancerSuite(t *testing.T) {
	suite.Run(t, new(testBalancerSuite))
}

// clear keys in etcd test cluster.
func (t *testBalancerSuite) clearTestInfoOperation() {
	t.T().Helper()
	require.NoError(t.T(), ha.ClearTestInfoOperation(t.etcdTestCli))
}

type testBalancerSuite struct {
	suite.Suite
	mockCluster *integration.ClusterV3
	etcdTestCli *clientv3.Client
}

func (t *testBalancerSuite) SetupSuite() {
	require.NoError(t.T(), log.InitLogger(&log.Config{Level: "debug"}))

	integration.BeforeTestExternal(t.T())
	t.mockCluster = integration.NewClusterV3(t.T(), &integration.ClusterConfig{Size: 1})
	t.etcdTestCli = t.mockCluster.RandClient()

}

func (t *testBalancerSuite) TearDownSuite() {
	t.mockCluster.Terminate(t.T())
}
func (t *testBalancerSuite) TearDownTest() {
	t.clearTestInfoOperation()
}

func (t *testBalancerSuite) TestTableNumberBalancer() {
	var (
		logger       = log.L()
		s            = NewScheduler(&logger, config.Security{})
		ctx, cancel  = context.WithCancel(context.Background())
		wg           = sync.WaitGroup{}
		keepAliveTTL = int64(5) // NOTE: this should be >= minLeaseTTL, in second.
	)
	cancels := make([]context.CancelFunc, 0, 4)
	defer func() {
		cancel()
		for _, cancel1 := range cancels {
			cancel1()
		}
		wg.Wait()
	}()

	// 1. start scheduler and rebalancer
	require.NoError(t.T(), s.Start(ctx, t.etcdTestCli))

	sourceID := 1
	boundWorkers := func(wNumStart int, wNumEndBound int) {
		for wNum := wNumStart; wNum <= 4; wNum++ {
			ctx1, cancel1 := context.WithCancel(ctx)
			require.NoError(t.T(), s.AddWorker(genWorkerName(wNum), genWorkerAddr(wNum)))
			cancels = append(cancels, cancel1)
			wg.Add(1)
			go func() {
				defer wg.Done()
				require.NoError(t.T(), ha.KeepAlive(ctx1, t.etcdTestCli, genWorkerName(wNum), keepAliveTTL))
			}()
			require.True(t.T(), utils.WaitSomething(30, 100*time.Millisecond, func() bool {
				kam, _, err := ha.GetKeepAliveWorkers(t.etcdTestCli)
				return err == nil && len(kam) == wNum
			}))
			if wNum >= wNumEndBound {
				continue
			}
			// bound 5,4,3 sources to dm-worker 1,2,3, and 0 sources to worker4
			for i := sourceID; i < sourceID+6-wNum; i++ {
				err := s.AddSourceCfg(genSourceCfg(t.T(), i))
				require.NoError(t.T(), err)
			}
			sourceID += 6 - wNum
		}
	}
	// 2. start dm-worker1 and bound 5,4,3 sources to dm-worker1,2,3
	boundWorkers(1, 4)
	// 3. trigger a rebalance, and sources should be balanced like 3,3,3,3
	require.True(t.T(), s.TriggerRebalance())
	require.True(t.T(), utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		workerName := genWorkerName(4)
		sbm, _, err := ha.GetSourceBound(t.etcdTestCli, workerName, "")
		if err != nil {
			log.L().Error("fail to get source bounds from etcd", zap.Error(err))
		}
		return len(sbm[workerName]) == 3
	}))
	expectRange := [][]int{{1, 5}, {6, 9}, {10, 12}}
	for wNum := 1; wNum <= 4; wNum++ {
		// when using s.GetWorkerByName we will apply a read lock on scheduler. If we can access it successfully,
		// it means the rebalance work is finished.
		w := s.GetWorkerByName(genWorkerName(wNum))
		require.Len(t.T(), w.Bounds(), 3)

		rangeSourceNum := make([]int, 2)
		for _, bound := range w.Bounds() {
			sourceNumStr := strings.TrimLeft(bound.Source, "mysql-replica-")
			sourceNum, err := strconv.Atoi(sourceNumStr)
			require.NoError(t.T(), err)
			if wNum <= 3 {
				l, r := expectRange[wNum-1][0], expectRange[wNum-1][1]
				require.GreaterOrEqual(t.T(), sourceNum, l)
				require.LessOrEqual(t.T(), sourceNum, r)
			} else {
				for i := 0; i <= 1; i++ {
					if sourceNum >= expectRange[i][0] && sourceNum <= expectRange[i][1] {
						rangeSourceNum[i]++
					}
				}
			}
		}
		if wNum >= 4 {
			require.Equal(t.T(), 2, rangeSourceNum[0])
			require.Equal(t.T(), 1, rangeSourceNum[1])
		}
	}

	// 4. stop worker 4 and trigger rebalance, should become 4,4,4,0
	cancels[3]()
	require.True(t.T(), utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		kam, _, err := ha.GetKeepAliveWorkers(t.etcdTestCli)
		return err == nil && len(kam) == 3
	}))

	require.True(t.T(), s.TriggerRebalance())
	require.True(t.T(), utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		workerName := genWorkerName(3)
		sbm, _, err := ha.GetSourceBound(t.etcdTestCli, workerName, "")
		if err != nil {
			log.L().Error("fail to get source bounds from etcd", zap.Error(err))
		}
		return len(sbm[workerName]) == 4
	}))
	for wNum := 1; wNum <= 3; wNum++ {
		w := s.GetWorkerByName(genWorkerName(wNum))
		require.Len(t.T(), w.Bounds(), 4)
	}
	require.Len(t.T(), s.GetWorkerByName(genWorkerName(4)).Bounds(), 0)

	// 5. stop worker 3 and trigger rebalance, should become 6,6,0,0
	cancels[2]()
	require.True(t.T(), utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		kam, _, err := ha.GetKeepAliveWorkers(t.etcdTestCli)
		return err == nil && len(kam) == 2
	}))
	require.True(t.T(), s.TriggerRebalance())
	require.True(t.T(), utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		workerName := genWorkerName(2)
		sbm, _, err := ha.GetSourceBound(t.etcdTestCli, workerName, "")
		if err != nil {
			log.L().Error("fail to get source bounds from etcd", zap.Error(err))
		}
		return len(sbm[workerName]) == 6
	}))
	for wNum := 1; wNum <= 2; wNum++ {
		w := s.GetWorkerByName(genWorkerName(wNum))
		require.Len(t.T(), w.Bounds(), 6)
	}
	require.Len(t.T(), s.GetWorkerByName(genWorkerName(3)).Bounds(), 0)
	require.Len(t.T(), s.GetWorkerByName(genWorkerName(4)).Bounds(), 0)

	// 6. start these two workers again, should become 3,3,3,3 after rebalance
	sourceID = 3
	cancels = cancels[:2]
	boundWorkers(3, 3)
	require.True(t.T(), s.TriggerRebalance())
	require.True(t.T(), utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		workerName := genWorkerName(4)
		sbm, _, err := ha.GetSourceBound(t.etcdTestCli, workerName, "")
		if err != nil {
			log.L().Error("fail to get source bounds from etcd", zap.Error(err))
		}
		return len(sbm[workerName]) == 3
	}))
	for wNum := 1; wNum <= 4; wNum++ {
		w := s.GetWorkerByName(genWorkerName(wNum))
		require.Len(t.T(), w.Bounds(), 3)
	}
	s.Close()
}

func (t *testBalancerSuite) TestTableNumberBalancerWithPrivileges() {
	var (
		logger       = log.L()
		s            = NewScheduler(&logger, config.Security{})
		ctx, cancel  = context.WithCancel(context.Background())
		wg           = sync.WaitGroup{}
		keepAliveTTL = int64(10) // NOTE: this should be >= minLeaseTTL, in second.
	)
	cancels := make([]context.CancelFunc, 0, 4)
	defer func() {
		cancel()
		for _, cancel1 := range cancels {
			cancel1()
		}
		wg.Wait()
	}()

	checkSources := func(expectBounds [][]int) {
		for wNum := 1; wNum <= len(expectBounds); wNum++ {
			// when using s.GetWorkerByName we will apply a read lock on scheduler. If we can access it successfully,
			// it means the rebalance work is finished.
			w := s.GetWorkerByName(genWorkerName(wNum))
			sources := expectBounds[wNum-1]
			require.Len(t.T(), w.Bounds(), len(sources))
			if len(sources) == 0 {
				continue
			}
			sourceIDs := make([]int, 0, len(sources))
			for _, bound := range w.Bounds() {
				sourceNumStr := strings.TrimLeft(bound.Source, "mysql-replica-")
				sourceID, err := strconv.Atoi(sourceNumStr)
				require.NoError(t.T(), err)
				sourceIDs = append(sourceIDs, sourceID)
			}
			sort.Ints(sourceIDs)
			require.Equal(t.T(), sources, sourceIDs)
		}
	}

	// 1. start scheduler and rebalancer
	require.NoError(t.T(), s.Start(ctx, t.etcdTestCli))

	// 2. start 4 workers, bound sources, start load tasks and relay on these workers
	sourceBoundInfo := [][]int{
		// sources to bound, sources with load tasks, sources with relay
		{5, 2, 2},
		{5, 1, 3},
		{2, 0, 0},
	}

	sourceID := 1
	for wNum := 1; wNum <= 4; wNum++ {
		ctx1, cancel1 := context.WithCancel(ctx)
		require.NoError(t.T(), s.AddWorker(genWorkerName(wNum), genWorkerAddr(wNum)))
		cancels = append(cancels, cancel1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t.T(), ha.KeepAlive(ctx1, t.etcdTestCli, genWorkerName(wNum), keepAliveTTL))
		}()
		require.True(t.T(), utils.WaitSomething(30, 100*time.Millisecond, func() bool {
			kam, _, err := ha.GetKeepAliveWorkers(t.etcdTestCli)
			return err == nil && len(kam) == wNum
		}))
		if wNum >= 4 {
			continue
		}
		boundInfo := sourceBoundInfo[wNum-1]
		sourcesToBound, sourcesLoadTask, sourcesRelay := boundInfo[0], boundInfo[1], boundInfo[2]

		// bound sources to dm-worker
		for i := sourceID; i < sourceID+sourcesToBound; i++ {
			err := s.AddSourceCfg(genSourceCfg(t.T(), i))
			require.NoError(t.T(), err)
		}

		if sourcesLoadTask > 0 {
			subtaskConfigs := make(map[string]config.SubTaskConfig)
			sourceWorkerMap := make(map[string]string)
			for i := sourceID; i < sourceID+sourcesLoadTask; i++ {
				subtaskConfigs[genSourceID(i)] = config.SubTaskConfig{}
				sourceWorkerMap[genSourceID(i)] = genWorkerName(wNum)
			}
			s.subTaskCfgs.Store(genTaskName(wNum), subtaskConfigs)
			s.loadTasks[genTaskName(wNum)] = sourceWorkerMap
		}
		if sourcesRelay > 0 {
			for i := sourceID; i < sourceID+sourcesLoadTask+sourcesRelay; i++ {
				require.NoError(t.T(), s.StartRelay(genSourceID(i), []string{genWorkerName(wNum)}))
			}
		}
		sourceID += sourcesToBound
	}

	// 3. trigger a rebalance, and sources should be balanced like 4(2load 2relay),4(1load 3relay),2,2
	require.True(t.T(), s.TriggerRebalance())
	utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		workerName := genWorkerName(4)
		sbm, _, err := ha.GetSourceBound(t.etcdTestCli, workerName, "")
		if err != nil {
			log.L().Error("fail to get source bounds from etcd", zap.Error(err))
		}
		return len(sbm[workerName]) == 2
	})
	require.Len(t.T(), s.GetWorkerByName(genWorkerName(4)).Bounds(), 2)
	checkSources([][]int{
		{1, 2, 3, 4},
		{6, 7, 8, 9},
		{11, 12},
		{5, 10},
	})

	// 4. trigger a rebalance, and sources should be balanced like 4(2load 2relay),4(1load 3relay),2,2
	require.NoError(t.T(), s.StartRelay(genSourceID(4), []string{genWorkerName(3)}))
	require.NoError(t.T(), s.StartRelay(genSourceID(9), []string{genWorkerName(4)})) // 3. trigger a rebalance, and sources should be balanced like 4(2load 2relay),4(1load 3relay),2,2
	require.True(t.T(), s.TriggerRebalance())
	require.True(t.T(), utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		workerName := genWorkerName(4)
		sbm, _, err := ha.GetSourceBound(t.etcdTestCli, workerName, "")
		if err != nil {
			log.L().Error("fail to get source bounds from etcd", zap.Error(err))
		}
		return len(sbm[workerName]) == 3
	}))

	checkSources([][]int{
		{1, 2, 3},
		{6, 7, 8},
		{4, 11, 12},
		{5, 9, 10},
	})

	// 4. stop worker 4 and trigger rebalance, should become 4,4,4,0
	// make sure source 5 can be bound to worker 1
	require.NoError(t.T(), s.StartRelay(genSourceID(5), []string{genWorkerName(1)}))
	cancels[3]()
	cancels = cancels[:len(cancels)-1]
	require.True(t.T(), utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		kam, _, err := ha.GetKeepAliveWorkers(t.etcdTestCli)
		return err == nil && len(kam) == 3
	}))
	s.TriggerRebalance()
	require.True(t.T(), utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		workerName := genWorkerName(1)
		sbm, _, err := ha.GetSourceBound(t.etcdTestCli, workerName, "")
		log.L().Info("get sbm", zap.Any("sbm", sbm))
		if err != nil {
			log.L().Error("fail to get source bounds from etcd", zap.Error(err))
		}
		return len(sbm[workerName]) == 4
	}))
	checkSources([][]int{
		{1, 2, 3, 5},
		{6, 7, 8, 9},
		{4, 10, 11, 12},
		{},
	})

	ctx1, cancel1 := context.WithCancel(ctx)
	cancels = append(cancels, cancel1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t.T(), ha.KeepAlive(ctx1, t.etcdTestCli, genWorkerName(4), keepAliveTTL))
	}()
	require.True(t.T(), utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		kam, _, err := ha.GetKeepAliveWorkers(t.etcdTestCli)
		return err == nil && len(kam) == 4
	}))
	require.NoError(t.T(), s.StartRelay(genSourceID(10), []string{genWorkerName(3)}))
	require.NoError(t.T(), s.StartRelay(genSourceID(11), []string{genWorkerName(3)}))
	for wNum := 1; wNum <= 2; wNum++ {
		subtaskConfigs := make(map[string]config.SubTaskConfig)
		sourceWorkerMap := make(map[string]string)
		for _, bound := range s.workers[genWorkerName(wNum)].Bounds() {
			subtaskConfigs[bound.Source] = config.SubTaskConfig{}
			sourceWorkerMap[bound.Source] = genWorkerName(wNum)
		}
		s.subTaskCfgs.Store(genTaskName(wNum), subtaskConfigs)
		s.loadTasks[genTaskName(wNum)] = sourceWorkerMap
	}
	s.TriggerRebalance()
	require.True(t.T(), utils.WaitSomething(30, 100*time.Millisecond, func() bool {
		workerName := genWorkerName(3)
		sbm, _, err := ha.GetSourceBound(t.etcdTestCli, workerName, "")
		log.L().Info("get sbm", zap.Any("sbm", sbm))
		if err != nil {
			log.L().Error("fail to get source bounds from etcd", zap.Error(err))
		}
		return len(sbm[workerName]) == 3
	}))
	checkSources([][]int{
		{1, 2, 3, 5},
		{6, 7, 8, 9},
		{4, 10, 11},
		{12},
	})
}
