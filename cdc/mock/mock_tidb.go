package mock

import (
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/parser"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/tidb/domain"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/util/testkit"
	"go.uber.org/zap"

	"github.com/pingcap/check"
)

// TiDB is used in tests to act as TiDB
// TODO deprecate this Mock TiDB
type TiDB struct {
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
	store     tidbkv.Storage
	domain    *domain.Domain
	*parser.Parser

	kvs map[string]string
}

// NewMockPuller creates a mock puller
func NewMockPuller() (*TiDB, error) {
	p := &TiDB{
		kvs: make(map[string]string),
	}

	if err := p.setUp(); err != nil {
		return nil, err
	}
	if _, err := p.updateEvent(); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *TiDB) setUp() (err error) {
	p.Parser = parser.New()
	p.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(p.cluster)
	p.mvccStore = mocktikv.MustNewMVCCStore()
	store, err := mockstore.NewMockTikvStore(
		mockstore.WithCluster(p.cluster),
		mockstore.WithMVCCStore(p.mvccStore),
	)
	if err != nil {
		return
	}

	p.store = store
	session.SetSchemaLease(0)
	session.DisableStats4Test()
	p.domain, err = session.BootstrapSession(p.store)
	if err != nil {
		return
	}

	p.domain.SetStatsUpdating(true)
	return nil
}

func (p *TiDB) tearDown() {
	p.domain.Close()
	p.store.Close()
}

// We scan all the KV space to get the changed KV events every time after
// execute a SQL and use the current version as the ts of the KV events
// because there's no way to get the true commit ts of the kv
func (p *TiDB) updateEvent() (entrys []*model.RawKVEntry, err error) {
	ver, err := p.store.CurrentVersion()
	if err != nil {
		return nil, err
	}

	ts := ver.Ver

	pairs := p.mvccStore.Scan(nil, nil, 1<<30, ts, kvrpcpb.IsolationLevel_RC)

	newKVS := make(map[string]string)
	for _, pair := range pairs {
		if pair.Err != nil {
			log.Error("pair err", zap.Error(pair.Err))
			continue
		}
		newKVS[string(pair.Key)] = string(pair.Value)
	}

	// Put kv
	for k, v := range newKVS {
		if p.kvs[k] != v {
			entry := model.RawKVEntry{
				OpType: model.OpTypePut,
				Key:    []byte(k),
				Value:  []byte(v),
				Ts:     ts,
			}

			entrys = append(entrys, &entry)
		}
	}

	// Delete
	for k := range p.kvs {
		_, ok := newKVS[k]
		if !ok {
			entry := model.RawKVEntry{
				OpType: model.OpTypeDelete,
				Key:    []byte(k),
				Ts:     ts,
			}
			entrys = append(entrys, &entry)
		}
	}

	p.kvs = newKVS

	return
}

// MustExec execute the sql and return all the KVEntry events
func (p *TiDB) MustExec(c *check.C, sql string, args ...interface{}) []*model.RawKVEntry {
	tk := testkit.NewTestKit(c, p.store)
	tk.MustExec(sql, args...)

	entrys, err := p.updateEvent()
	c.Assert(err, check.IsNil)

	return entrys
}

// ScanAll scans all key value pairs and call the provided function with RawKVEntry one by one
func (p *TiDB) ScanAll(fn func(*model.RawKVEntry)) {
	for key, value := range p.kvs {
		fn(&model.RawKVEntry{Key: []byte(key), Value: []byte(value), OpType: model.OpTypePut})
	}
}

// GetAllHistoryDDLJobs returns all finished DDL Jobs
func (p *TiDB) GetAllHistoryDDLJobs() ([]*timodel.Job, error) {
	return kv.LoadHistoryDDLJobs(p.store)
}
