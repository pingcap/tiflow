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

package tp

import (
	"math"
	"math/rand"
	"sort"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

var _ scheduler = &rebalanceScheduler{}

type rebalanceScheduler struct {
	rebalance int32
	random    *rand.Rand
}

func newRebalanceScheduler() *rebalanceScheduler {
	return &rebalanceScheduler{
		rebalance: 0,
		random:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (r *rebalanceScheduler) Name() string {
	return string(schedulerTypeRebalance)
}

func (r *rebalanceScheduler) Schedule(
	checkpointTs model.Ts,
	currentTables []model.TableID,
	captures map[model.CaptureID]*model.CaptureInfo,
	replications map[model.TableID]*ReplicationSet,
) []*scheduleTask {
	// rebalance is not triggered, or there is still some pending task,
	// do not generate new tasks.
	if atomic.LoadInt32(&r.rebalance) == 0 {
		return nil
	}

	if len(captures) == 0 {
		return nil
	}

	// only rebalance when all tables are replicating
	for _, tableID := range currentTables {
		rep, ok := replications[tableID]
		if !ok {
			return nil
		}
		if rep.State != ReplicationSetStateReplicating {
			return nil
		}
	}

	accept := func() {
		atomic.StoreInt32(&r.rebalance, 0)
	}
	task := newBurstBalanceMoveTables(accept, r.random, captures, replications)
	if task == nil {
		return nil
	}
	return []*scheduleTask{task}
}

func newBurstBalanceMoveTables(
	accept callback,
	random *rand.Rand,
	captures map[model.CaptureID]*model.CaptureInfo,
	replications map[model.TableID]*ReplicationSet,
) *scheduleTask {
	tablesPerCapture := make(map[model.CaptureID]*tableSet)
	for captureID := range captures {
		tablesPerCapture[captureID] = newTableSet()
	}

	for tableID, rep := range replications {
		if rep.State != ReplicationSetStateReplicating {
			log.Info("tpscheduler: rebalance skip tables that are not in Replicating",
				zap.Int64("tableID", tableID),
				zap.Any("replication", rep))
			continue
		}
		tablesPerCapture[rep.Primary].add(tableID)
	}

	// findVictim return tables which need to be moved
	upperLimitPerCapture := int(math.Ceil(float64(len(replications)) / float64(len(captures))))

	victims := make([]model.TableID, 0)
	for _, ts := range tablesPerCapture {
		tables := ts.keys()
		if random != nil {
			// Complexity note: Shuffle has O(n), where `n` is the number of tables.
			// Also, during a single call of `Schedule`, Shuffle can be called at most
			// `c` times, where `c` is the number of captures (TiCDC nodes).
			// Only called when a rebalance is triggered, which happens rarely,
			// we do not expect a performance degradation as a result of adding
			// the randomness.
			random.Shuffle(len(tables), func(i, j int) {
				tables[i], tables[j] = tables[j], tables[i]
			})
		} else {
			// sort the tableIDs here so that the result is deterministic,
			// which would aid testing and debugging.
			sort.Slice(tables, func(i, j int) bool {
				return tables[i] < tables[j]
			})
		}

		tableNum2Remove := len(tables) - upperLimitPerCapture
		if tableNum2Remove <= 0 {
			continue
		}

		for _, table := range tables {
			if tableNum2Remove <= 0 {
				break
			}
			victims = append(victims, table)
			ts.remove(table)
			tableNum2Remove--
		}
	}
	if len(victims) == 0 {
		return nil
	}

	captureWorkload := make(map[model.CaptureID]int)
	for captureID, ts := range tablesPerCapture {
		captureWorkload[captureID] = randomizeWorkload(random, ts.size())
	}
	// for each victim table, find the target for it
	moveTables := make([]moveTable, 0, len(victims))
	for _, tableID := range victims {
		target := ""
		minWorkload := math.MaxInt64

		for captureID, workload := range captureWorkload {
			if workload < minWorkload {
				minWorkload = workload
				target = captureID
			}
		}

		if minWorkload == math.MaxInt64 {
			log.Panic("tpscheduler: rebalance meet unexpected min workload " +
				"when try to the the target capture")
		}

		moveTables = append(moveTables, moveTable{
			TableID:     tableID,
			DestCapture: target,
		})
		tablesPerCapture[target].add(tableID)
		captureWorkload[target] = randomizeWorkload(random, tablesPerCapture[target].size())
	}

	return &scheduleTask{
		burstBalance: &burstBalance{MoveTables: moveTables},
		accept:       accept,
	}
}

const (
	randomPartBitSize = 8
	randomPartMask    = (1 << randomPartBitSize) - 1
)

// randomizeWorkload injects small randomness into the workload, so that
// when two captures tied in competing for the minimum workload, the result
// will not always be the same.
// The bitwise layout of the return value is:
// 63                8                0
// |----- input -----|-- random val --|
func randomizeWorkload(random *rand.Rand, input int) int {
	var randomPart int
	if random != nil {
		randomPart = int(random.Uint32() & randomPartMask)
	}
	// randomPart is a small random value that only affects the
	// result of comparison of workloads when two workloads are equal.
	return (input << randomPartBitSize) | randomPart
}

type tableSet struct {
	memo map[model.TableID]struct{}
}

func newTableSet() *tableSet {
	return &tableSet{
		memo: make(map[model.TableID]struct{}),
	}
}

func (s *tableSet) add(tableID model.TableID) {
	s.memo[tableID] = struct{}{}
}

func (s *tableSet) remove(tableID model.TableID) {
	delete(s.memo, tableID)
}

func (s *tableSet) keys() []model.TableID {
	result := make([]model.TableID, 0, len(s.memo))
	for k := range s.memo {
		result = append(result, k)
	}
	return result
}

func (s *tableSet) size() int {
	return len(s.memo)
}
