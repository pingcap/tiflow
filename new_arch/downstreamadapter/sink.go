// Copyright 2024 PingCAP, Inc.
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

package downstreamadapter

import (
	"context"

	"github.com/pingcap/tiflow/pkg/causality"
)

type SinkType string

const (
	SinkTypeMysql SinkType = "mysql"
	SinkTypeTiDB  SinkType = "tidb"
	SinkTypeKafka SinkType = "kafka"

	workerCount = 8 // 先来个暴力固定
)

// 理论上他应该是个接口，每个不同的下游都需要实现他对外的接口
// 他负责接收达到下推要求的 txn level 的 events，然后把这些 events 正确的下入下游
// 也负责提供 flush 接口，把收到的 ddl / sync point event 直接 flush 到下游
type Sink interface {
	WriteTxnEvent(event *TxnEvent) error // for txn dml events
	FlushEvent(event *TxnEvent) error    // for ddl / sync point event
	IsEmpty() bool                       // indicates whether here is no data in sink buffer
	GetCheckpointTs() (uint64, error)
}

func createSink(sinkType SinkType) Sink {
	if sinkType == SinkTypeMysql || sinkType == SinkTypeTiDB {
		return newMysqlSink()
	} else {
		//
	}
}

// sink for mysql and tidb downstream
// TODO:如果出错或者挂了应该报错然后自己尝试重启？如果重试失败就应该直接状态出错
type MysqlSink struct {
	ctx              context.Context
	cancel           context.CancelFunc
	ConflictDetector *causality.ConflictDetector[*TxnEvent]
	Workers          []*Worker
}

func newMysqlSink(ctx context.Context) *MysqlSink {
	ctx, cancel := context.WithCancel(ctx)
	sink := MysqlSink{
		ConflictDetector: causality.NewConflictDetector[*TxnEvent](16*1024, // slot count
			causality.TxnCacheOption{
				Count:         workerCount,
				Size:          1024,
				BlockStrategy: causality.BlockStrategyWaitEmpty,
			}),
		Workers: make([]*Worker, workerCount),
		ctx:     ctx,
		cancel:  cancel,
	}
	for i := 0; i < workerCount; i++ {
		sink.Workers[i] = NewWorker(ctx, sink.ConflictDetector.GetOutChByCacheID(int64(i)), i)
	}
}

func (s *MysqlSink) WriteTxnEvent(event *TxnEvent) error {
	s.ConflictDetector.Add(event)
}

func (s *MysqlSink) FlushEvent(event *TxnEvent) error {
	// 随便选一个 worker 调用 flush event
}

func (s *MysqlSink) IsEmpty() bool {
	// conflict 和 所有 worker 都是没有数据在里面的情况
	if !s.ConflictDetector.IsEmpty() { // todo: implement it
		return false
	}

	for _, worker := range s.Workers {
		if !worker.IsEmpty() {
			return false
		}
	}

	return true
}

func (s *MysqlSink) GetCheckpointTs() (uint64, error) {
	// 每个 worker 存一下，拿的时候汇总一下就行
}
