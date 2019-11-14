package puller

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-cdc/cdc/kv"
	"github.com/pingcap/tidb-cdc/cdc/txn"
	"github.com/pingcap/tidb-cdc/pkg/util"
	"github.com/pingcap/tidb/domain"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/testkit"
	"go.uber.org/zap"
)

type MVCCListener struct {
	mocktikv.MVCCStore
	mu           sync.RWMutex
	postPrewrite func(req *kvrpcpb.PrewriteRequest, result []error)
	postCommit   func(keys [][]byte, startTs, commitTs uint64, result error)
	postRollback func(keys [][]byte, startTs uint64, result error)
}

func NewMVCCListener(store mocktikv.MVCCStore) *MVCCListener {
	return &MVCCListener{
		MVCCStore:    store,
		postPrewrite: func(_ *kvrpcpb.PrewriteRequest, _ []error) {},
		postCommit:   func(_ [][]byte, _, _ uint64, _ error) {},
		postRollback: func(_ [][]byte, _ uint64, _ error) {},
	}
}

func (l *MVCCListener) Prewrite(req *kvrpcpb.PrewriteRequest) []error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	result := l.MVCCStore.Prewrite(req)
	log.Debug("MVCCListener Prewrite", zap.Reflect("req", req), zap.Reflect("result", result))
	l.postPrewrite(req, result)
	return result
}
func (l *MVCCListener) Commit(keys [][]byte, startTs, commitTs uint64) error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	result := l.MVCCStore.Commit(keys, startTs, commitTs)
	log.Debug("MVCCListener Commit", zap.Reflect("keys", keys),
		zap.Uint64("startTs", startTs),
		zap.Uint64("commitTs", commitTs),
		zap.Reflect("result", result))
	l.postCommit(keys, startTs, commitTs, result)
	return result
}
func (l *MVCCListener) Rollback(keys [][]byte, startTs uint64) error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	result := l.MVCCStore.Rollback(keys, startTs)
	log.Debug("MVCCListener Commit", zap.Reflect("keys", keys),
		zap.Uint64("startTs", startTs),
		zap.Reflect("result", result))
	l.postRollback(keys, startTs, result)
	return result
}

func (l *MVCCListener) RegisterPostPrewrite(fn func(req *kvrpcpb.PrewriteRequest, result []error)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.postPrewrite = fn
}

func (l *MVCCListener) RegisterPostCommit(fn func(keys [][]byte, startTs, commitTs uint64, result error)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.postCommit = fn
}

func (l *MVCCListener) RegisterPostRollback(fn func(keys [][]byte, startTs uint64, result error)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.postRollback = fn
}

type MockPullerManager struct {
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
	store     tidbkv.Storage
	domain    *domain.Domain

	txnMap   map[uint64]*kvrpcpb.PrewriteRequest
	rawTxnCh chan txn.RawTxn
	tidbKit  *testkit.TestKit

	rawTxns []txn.RawTxn

	txnMapMu  sync.Mutex
	rawTxnsMu sync.RWMutex
	closeCh   chan struct{}

	c *check.C
}

type mockPuller struct {
	pm         *MockPullerManager
	spans      []util.Span
	resolvedTs uint64
	startTs    uint64
	txnOffset  int
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

func (p *mockPuller) CollectRawTxns(ctx context.Context, outputFn func(context.Context, txn.RawTxn) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.pm.closeCh:
			return nil
		case <-time.After(time.Second):
			p.pm.rawTxnsMu.RLock()
			for ; p.txnOffset < len(p.pm.rawTxns); p.txnOffset++ {
				rawTxn := p.pm.rawTxns[p.txnOffset]
				if rawTxn.Ts < p.startTs {
					continue
				}
				p.resolvedTs = rawTxn.Ts
				p.sendRawTxn(ctx, rawTxn, outputFn)
			}
			p.pm.rawTxnsMu.RUnlock()
		}
	}
}

func (p *mockPuller) Output() Buffer {
	panic("unreachable")
}

func NewMockPullerManager(c *check.C) *MockPullerManager {
	m := &MockPullerManager{
		txnMap:   make(map[uint64]*kvrpcpb.PrewriteRequest),
		rawTxnCh: make(chan txn.RawTxn, 16),
		closeCh:  make(chan struct{}),
		c:        c,
	}
	return m
}

func (m *MockPullerManager) setUp() {
	m.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(m.cluster)

	mvccListener := NewMVCCListener(mocktikv.MustNewMVCCStore())

	m.mvccStore = mvccListener
	store, err := mockstore.NewMockTikvStore(
		mockstore.WithCluster(m.cluster),
		mockstore.WithMVCCStore(m.mvccStore),
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

	mvccListener.RegisterPostPrewrite(m.postPrewrite)
	mvccListener.RegisterPostCommit(m.postCommit)
	mvccListener.RegisterPostRollback(m.postRollback)
}

func (m *MockPullerManager) Run(ctx context.Context) {
	m.setUp()
	go func() {
		for {
			select {
			case <-ctx.Done():
				m.domain.Close()
				m.store.Close()
				close(m.closeCh)
				return
			case r, ok := <-m.rawTxnCh:
				log.Info("send raw transaction", zap.Reflect("raw transaction", r))
				if !ok {
					return
				}
				m.rawTxnsMu.Lock()
				m.rawTxns = append(m.rawTxns, r)
				m.rawTxnsMu.Unlock()
			case <-time.After(time.Second):
				log.Info("send fake transaction")
				m.rawTxnsMu.Lock()
				fakeTxn := txn.RawTxn{Ts: oracle.EncodeTSO(time.Now().UnixNano() / int64(time.Millisecond))}
				m.rawTxns = append(m.rawTxns, fakeTxn)
				m.rawTxnsMu.Unlock()
			}
		}
	}()
}

func (m *MockPullerManager) CreatePuller(startTs uint64, spans []util.Span) Puller {
	return &mockPuller{
		spans:   spans,
		pm:      m,
		startTs: startTs,
	}
}

func (m *MockPullerManager) MustExec(sql string, args ...interface{}) {
	m.tidbKit.MustExec(sql, args...)
}

func (p *mockPuller) sendRawTxn(ctx context.Context, rawTxn txn.RawTxn, outputFn func(context.Context, txn.RawTxn) error) {
	toSend := txn.RawTxn{Ts: rawTxn.Ts}
	if len(rawTxn.Entries) > 0 {
		for _, kvEntry := range rawTxn.Entries {
			if util.KeyInSpans(kvEntry.Key, p.spans) {
				toSend.Entries = append(toSend.Entries, kvEntry)
			}
		}
		if len(toSend.Entries) == 0 {
			return
		}
	}
	err := outputFn(ctx, toSend)
	if err != nil {
		log.Fatal("output raw transaction failed", zap.Error(err))
	}
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
	defer m.txnMapMu.Unlock()
	if result != nil {
		return
	}
	prewrite, exist := m.txnMap[startTs]
	if !exist {
		log.Fatal("txn not found", zap.Uint64("startTs", startTs))
	}
	delete(m.txnMap, startTs)
	m.rawTxnCh <- prewrite2RawTxn(prewrite, commitTs)
}

func (m *MockPullerManager) postRollback(keys [][]byte, startTs uint64, result error) {
	m.txnMapMu.Lock()
	defer m.txnMapMu.Unlock()
	if result != nil {
		return
	}
	delete(m.txnMap, startTs)
}

func prewrite2RawTxn(req *kvrpcpb.PrewriteRequest, commitTs uint64) txn.RawTxn {
	var entries []*kv.RawKVEntry
	for _, mut := range req.Mutations {
		var op kv.OpType
		switch mut.Op {
		case kvrpcpb.Op_Put, kvrpcpb.Op_Insert:
			op = kv.OpTypePut
		case kvrpcpb.Op_Del:
			op = kv.OpTypeDelete
		default:
			continue
		}
		rawKV := &kv.RawKVEntry{
			Ts:     commitTs,
			Key:    mut.Key,
			Value:  mut.Value,
			OpType: op,
		}
		entries = append(entries, rawKV)
	}
	return txn.RawTxn{Ts: commitTs, Entries: entries}
}

func anyError(errs []error) bool {
	for _, err := range errs {
		if err != nil {
			return true
		}
	}
	return false
}
