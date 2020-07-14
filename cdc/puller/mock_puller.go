// Copyright 2020 PingCAP, Inc.
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

package puller

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/tidb/domain"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/cluster"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/util/testkit"
	"go.uber.org/zap"
)

type mvccListener struct {
	mocktikv.MVCCStore
	mu           sync.RWMutex
	postPrewrite func(req *kvrpcpb.PrewriteRequest, result []error)
	postCommit   func(keys [][]byte, startTs, commitTs uint64, result error)
	postRollback func(keys [][]byte, startTs uint64, result error)
}

func newMVCCListener(store mocktikv.MVCCStore) *mvccListener {
	return &mvccListener{
		MVCCStore:    store,
		postPrewrite: func(_ *kvrpcpb.PrewriteRequest, _ []error) {},
		postCommit:   func(_ [][]byte, _, _ uint64, _ error) {},
		postRollback: func(_ [][]byte, _ uint64, _ error) {},
	}
}

// Prewrite implements the MVCCStore interface
func (l *mvccListener) Prewrite(req *kvrpcpb.PrewriteRequest) []error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	result := l.MVCCStore.Prewrite(req)
	log.Debug("mvccListener Prewrite", zap.Reflect("req", req), zap.Reflect("result", result))
	l.postPrewrite(req, result)
	return result
}

// Commit implements the MVCCStore interface
func (l *mvccListener) Commit(keys [][]byte, startTs, commitTs uint64) error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	result := l.MVCCStore.Commit(keys, startTs, commitTs)
	log.Debug("mvccListener Commit", zap.Reflect("keys", keys),
		zap.Uint64("startTs", startTs),
		zap.Uint64("commitTs", commitTs),
		zap.Reflect("result", result))
	l.postCommit(keys, startTs, commitTs, result)
	return result
}

// Rollback implements the MVCCStore interface
func (l *mvccListener) Rollback(keys [][]byte, startTs uint64) error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	result := l.MVCCStore.Rollback(keys, startTs)
	log.Debug("mvccListener Commit", zap.Reflect("keys", keys),
		zap.Uint64("startTs", startTs),
		zap.Reflect("result", result))
	l.postRollback(keys, startTs, result)
	return result
}

func (l *mvccListener) registerPostPrewrite(fn func(req *kvrpcpb.PrewriteRequest, result []error)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.postPrewrite = fn
}

func (l *mvccListener) registerPostCommit(fn func(keys [][]byte, startTs, commitTs uint64, result error)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.postCommit = fn
}

func (l *mvccListener) registerPostRollback(fn func(keys [][]byte, startTs uint64, result error)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.postRollback = fn
}

// MockPullerManager keeps track of transactions for mock pullers
type MockPullerManager struct {
	cluster   cluster.Cluster
	mvccStore mocktikv.MVCCStore
	store     tidbkv.Storage
	domain    *domain.Domain

	txnMap  map[uint64]*kvrpcpb.PrewriteRequest
	tidbKit *testkit.TestKit

	rawKVEntries []*model.RawKVEntry

	txnMapMu       sync.Mutex
	rawKVEntriesMu sync.RWMutex
	closeCh        chan struct{}

	c *check.C
}

var _ Puller = &mockPuller{}

type mockPuller struct {
	pm          *MockPullerManager
	spans       []regionspan.ComparableSpan
	resolvedTs  uint64
	startTs     uint64
	rawKVOffset int
}

func (p *mockPuller) Output() <-chan *model.RawKVEntry {
	panic("implement me")
}

func (p *mockPuller) SortedOutput(ctx context.Context) <-chan *model.RawKVEntry {
	output := make(chan *model.RawKVEntry, 16)
	go func() {
		for {
			p.pm.rawKVEntriesMu.RLock()
			for ; p.rawKVOffset < len(p.pm.rawKVEntries); p.rawKVOffset++ {
				rawKV := p.pm.rawKVEntries[p.rawKVOffset]
				if rawKV.StartTs < p.startTs {
					continue
				}
				p.resolvedTs = rawKV.StartTs
				if !regionspan.KeyInSpans(rawKV.Key, p.spans) {
					continue
				}
				select {
				case <-ctx.Done():
					return
				case output <- rawKV:
				}
			}
			p.pm.rawKVEntriesMu.RUnlock()
			time.Sleep(time.Millisecond * 100)
		}
	}()
	return output
}

func (p *mockPuller) Run(ctx context.Context) error {
	// Do nothing
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.pm.closeCh:
		return nil
	}
}

func (p *mockPuller) GetResolvedTs() uint64 {
	return p.resolvedTs
}

// NewMockPullerManager creates and sets up a mock puller manager
func NewMockPullerManager(c *check.C, newRowFormat bool) *MockPullerManager {
	m := &MockPullerManager{
		txnMap:  make(map[uint64]*kvrpcpb.PrewriteRequest),
		closeCh: make(chan struct{}),
		c:       c,
	}
	m.setUp(newRowFormat)
	return m
}

func (m *MockPullerManager) setUp(newRowFormat bool) {
	// avoid to print too many logs
	logLevel := log.GetLevel()
	log.SetLevel(zap.FatalLevel)
	defer log.SetLevel(logLevel)

	mvccListener := newMVCCListener(mocktikv.MustNewMVCCStore())

	m.mvccStore = mvccListener
	store, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c cluster.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			m.cluster = c
		}),
	)
	if err != nil {
		log.Fatal("create mock puller failed", zap.Error(err))
	}
	m.store = store

	session.SetSchemaLease(0)
	session.DisableStats4Test()
	m.domain, err = session.BootstrapSession(m.store)
	if err != nil {
		log.Fatal("create mock puller failed", zap.Error(err))
	}

	m.domain.SetStatsUpdating(true)

	m.tidbKit = testkit.NewTestKit(m.c, m.store)
	m.MustExec("use test;")
	m.tidbKit.Se.GetSessionVars().RowEncoder.Enable = newRowFormat

	mvccListener.registerPostPrewrite(m.postPrewrite)
	mvccListener.registerPostCommit(m.postCommit)
	mvccListener.registerPostRollback(m.postRollback)
}

// CreatePuller returns a mock puller with the specified start ts and spans
func (m *MockPullerManager) CreatePuller(startTs uint64, spans []regionspan.ComparableSpan) Puller {
	return &mockPuller{
		spans:   spans,
		pm:      m,
		startTs: startTs,
	}
}

// MustExec delegates to TestKit.MustExec
func (m *MockPullerManager) MustExec(sql string, args ...interface{}) {
	m.tidbKit.MustExec(sql, args...)
}

// GetTableInfo queries the info schema with the table name and returns the TableInfo
func (m *MockPullerManager) GetTableInfo(schemaName, tableName string) *entry.TableInfo {
	is := m.domain.InfoSchema()
	tbl, err := is.TableByName(timodel.NewCIStr(schemaName), timodel.NewCIStr(tableName))
	m.c.Assert(err, check.IsNil)
	dbInfo, exist := is.SchemaByTable(tbl.Meta())
	m.c.Assert(exist, check.IsTrue)
	return entry.WrapTableInfo(dbInfo.ID, dbInfo.Name.O, tbl.Meta())
}

func (m *MockPullerManager) postPrewrite(req *kvrpcpb.PrewriteRequest, result []error) {
	m.txnMapMu.Lock()
	defer m.txnMapMu.Unlock()
	if anyError(result) {
		return
	}
	m.txnMap[req.StartVersion] = req
}

func (m *MockPullerManager) postCommit(keys [][]byte, startTs, commitTs uint64, result error) {
	m.txnMapMu.Lock()
	if result != nil {
		return
	}
	prewrite, exist := m.txnMap[startTs]
	if !exist {
		log.Fatal("txn not found", zap.Uint64("startTs", startTs))
	}
	delete(m.txnMap, startTs)
	m.txnMapMu.Unlock()

	entries := prewrite2RawKV(prewrite, commitTs)
	m.rawKVEntriesMu.Lock()
	defer m.rawKVEntriesMu.Unlock()
	m.rawKVEntries = append(m.rawKVEntries, entries...)
}

func (m *MockPullerManager) postRollback(keys [][]byte, startTs uint64, result error) {
	m.txnMapMu.Lock()
	defer m.txnMapMu.Unlock()
	if result != nil {
		return
	}
	delete(m.txnMap, startTs)
}

func prewrite2RawKV(req *kvrpcpb.PrewriteRequest, commitTs uint64) []*model.RawKVEntry {
	var putEntries []*model.RawKVEntry
	var deleteEntries []*model.RawKVEntry
	for _, mut := range req.Mutations {
		switch mut.Op {
		case kvrpcpb.Op_Put, kvrpcpb.Op_Insert:
			rawKV := &model.RawKVEntry{
				StartTs: req.GetStartVersion(),
				CRTs:    commitTs,
				Key:     mut.Key,
				Value:   mut.Value,
				OpType:  model.OpTypePut,
			}
			putEntries = append(putEntries, rawKV)
		case kvrpcpb.Op_Del:
			rawKV := &model.RawKVEntry{
				StartTs: req.GetStartVersion(),
				CRTs:    commitTs,
				Key:     mut.Key,
				Value:   mut.Value,
				OpType:  model.OpTypeResolved,
			}
			deleteEntries = append(deleteEntries, rawKV)
		default:
			continue
		}
	}
	entries := append(deleteEntries, putEntries...)
	return append(entries, &model.RawKVEntry{CRTs: commitTs, OpType: model.OpTypeResolved})
}

func anyError(errs []error) bool {
	for _, err := range errs {
		if err != nil {
			return true
		}
	}
	return false
}
