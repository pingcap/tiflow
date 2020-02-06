package puller

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/schema"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/domain"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
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
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
	store     tidbkv.Storage
	domain    *domain.Domain

	txnMap   map[uint64]*kvrpcpb.PrewriteRequest
	rawTxnCh chan model.RawTxn
	tidbKit  *testkit.TestKit

	rawTxns []model.RawTxn

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

func (p *mockPuller) CollectRawTxns(ctx context.Context, outputFn func(context.Context, model.RawTxn) error) error {
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

// NewMockPullerManager creates and sets up a mock puller manager
func NewMockPullerManager(c *check.C, newRowFormat bool) *MockPullerManager {
	m := &MockPullerManager{
		txnMap:   make(map[uint64]*kvrpcpb.PrewriteRequest),
		rawTxnCh: make(chan model.RawTxn, 16),
		closeCh:  make(chan struct{}),
		c:        c,
	}
	m.setUp(newRowFormat)
	return m
}

func (m *MockPullerManager) setUp(newRowFormat bool) {
	// avoid to print too many logs
	logLevel := log.GetLevel()
	log.SetLevel(zap.FatalLevel)
	defer log.SetLevel(logLevel)

	m.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(m.cluster)

	mvccListener := newMVCCListener(mocktikv.MustNewMVCCStore())

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
	m.tidbKit.Se.GetSessionVars().RowEncoder.Enable = newRowFormat

	mvccListener.registerPostPrewrite(m.postPrewrite)
	mvccListener.registerPostCommit(m.postCommit)
	mvccListener.registerPostRollback(m.postRollback)
}

// Run watches and captures all committed rawTxns
func (m *MockPullerManager) Run(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				m.domain.Close()
				m.store.Close()
				close(m.closeCh)
				return
			case r, ok := <-m.rawTxnCh:
				m.c.Log("send raw transaction", r)
				if !ok {
					return
				}
				m.rawTxnsMu.Lock()
				m.rawTxns = append(m.rawTxns, r)
				m.rawTxnsMu.Unlock()
			case <-time.After(time.Second):
				m.rawTxnsMu.Lock()
				fakeTxn := model.RawTxn{Ts: oracle.EncodeTSO(time.Now().UnixNano() / int64(time.Millisecond))}
				m.rawTxns = append(m.rawTxns, fakeTxn)
				m.rawTxnsMu.Unlock()
			}
		}
	}()
}

// CreatePuller returns a mock puller with the specified start ts and spans
func (m *MockPullerManager) CreatePuller(startTs uint64, spans []util.Span) Puller {
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
func (m *MockPullerManager) GetTableInfo(schemaName, tableName string) *schema.TableInfo {
	is := m.domain.InfoSchema()
	tbl, err := is.TableByName(timodel.NewCIStr(schemaName), timodel.NewCIStr(tableName))
	m.c.Assert(err, check.IsNil)
	return schema.WrapTableInfo(tbl.Meta())
}

// GetDDLJobs returns the ddl jobs
func (m *MockPullerManager) GetDDLJobs() []*timodel.Job {
	jobs, err := kv.LoadHistoryDDLJobs(m.store)
	m.c.Assert(err, check.IsNil)
	return jobs
}

func (p *mockPuller) sendRawTxn(ctx context.Context, rawTxn model.RawTxn, outputFn func(context.Context, model.RawTxn) error) {
	toSend := model.RawTxn{Ts: rawTxn.Ts}
	if len(rawTxn.Entries) > 0 {
		for _, kvEntry := range rawTxn.Entries {
			if util.KeyInSpans(kvEntry.Key, p.spans, false) {
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

func prewrite2RawTxn(req *kvrpcpb.PrewriteRequest, commitTs uint64) model.RawTxn {
	var entries []*model.RawKVEntry
	for _, mut := range req.Mutations {
		var op model.OpType
		switch mut.Op {
		case kvrpcpb.Op_Put, kvrpcpb.Op_Insert:
			op = model.OpTypePut
		case kvrpcpb.Op_Del:
			op = model.OpTypeDelete
		default:
			continue
		}
		rawKV := &model.RawKVEntry{
			Ts:     commitTs,
			Key:    mut.Key,
			Value:  mut.Value,
			OpType: op,
		}
		entries = append(entries, rawKV)
	}
	return model.RawTxn{Ts: commitTs, Entries: entries}
}

func anyError(errs []error) bool {
	for _, err := range errs {
		if err != nil {
			return true
		}
	}
	return false
}
