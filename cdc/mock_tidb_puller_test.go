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

package cdc

import (
	"fmt"
	"github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"go.uber.org/zap"
	"reflect"
	"testing"
	"time"
)

type MockTiDBPuller struct {
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
	store     kv.Storage
	domain    *domain.Domain
	*parser.Parser
	ctx *mock.Context
	c   *check.C
	tk  *testkit.TestKit

	sqlChan chan string
	close   chan struct{}

	kvs map[string][]byte
}

func (m *MockTiDBPuller) Pull() <-chan RawKVEntry {
	output := make(chan RawKVEntry)
	go func() {
		for {
			select {
			case <-m.close:
				close(output)
				log.Info("MockTiDBPuller exited")
				return
			case <-time.After(50 * time.Millisecond):
				err := m.updateEvent(output)
				if err != nil {
					log.Error("updateEvent error", zap.Error(err))
				}
			}
		}
	}()
	return output
}

func NewMockTiDBPuller(c *check.C, sqlChan chan string) (*MockTiDBPuller, error) {
	m := &MockTiDBPuller{
		kvs:     make(map[string][]byte),
		sqlChan: sqlChan,
		close:   make(chan struct{}),
		c:       c,
	}

	m.Parser = parser.New()
	m.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(m.cluster)
	m.mvccStore = mocktikv.MustNewMVCCStore()
	store, err := mockstore.NewMockTikvStore(
		mockstore.WithCluster(m.cluster),
		mockstore.WithMVCCStore(m.mvccStore),
	)
	if err != nil {
		return nil, err
	}

	m.store = store
	session.SetSchemaLease(0)
	session.DisableStats4Test()
	m.domain, err = session.BootstrapSession(m.store)
	if err != nil {
		return nil, err
	}

	m.domain.SetStatsUpdating(true)
	if err := m.syncKvs(); err != nil {
		return nil, err
	}
	m.tk = testkit.NewTestKit(m.c, m.store)
	m.run()
	return m, nil
}

func (m *MockTiDBPuller) Close() {
	close(m.sqlChan)
	close(m.close)
}

func (m *MockTiDBPuller) scan(kvs map[string][]byte) (uint64, error) {
	ver, err := m.store.CurrentVersion()
	if err != nil {
		return 0, err
	}
	ts := ver.Ver

	pairs := m.mvccStore.Scan(nil, nil, 1<<30, ts, kvrpcpb.IsolationLevel_RC)
	for _, pair := range pairs {
		if pair.Err != nil {
			log.Error("pair err", zap.Error(pair.Err))
			continue
		}
		kvs[string(pair.Key)] = pair.Value
	}
	return ts, nil
}

func (m *MockTiDBPuller) syncKvs() error {
	if len(m.kvs) != 0 {
		m.kvs = make(map[string][]byte)
	}
	_, err := m.scan(m.kvs)
	println(len(m.kvs))
	return err
}

func (m *MockTiDBPuller) run() {
	go func() {
		for sql := range m.sqlChan {
			m.tk.MustExec(sql)
		}
	}()
}

// We scan all the KV space to get the changed KV events every time after
// execute a SQL and use the current version as the ts of the KV events
// because there's no way to get the true commit ts of the kv
func (m *MockTiDBPuller) updateEvent(kvChan chan<- RawKVEntry) error {
	newKVS := make(map[string][]byte)
	ts, err := m.scan(newKVS)
	if err != nil {
		return err
	}

	// Put kv
	for k, v := range newKVS {
		oldV, _ := m.kvs[k]
		if !reflect.DeepEqual(oldV, v) {
			entry := RawKVEntry{
				OpType: OpTypePut,
				Key:    []byte(k),
				Value:  v,
				Ts:     ts,
			}
			kvChan <- entry
		}
	}

	// Delete
	for k, v := range m.kvs {
		_, ok := newKVS[k]
		if !ok {
			entry := RawKVEntry{
				OpType: OpTypeDelete,
				Key:    []byte(k),
				Value:  v,
				Ts:     ts,
			}
			kvChan <- entry
		}
	}

	// sned resolved ts
	kvChan <- RawKVEntry{
		OpType: OpResolvedTS,
		Ts:     ts,
	}

	m.kvs = newKVS
	return nil
}

func Test(t *testing.T) { check.TestingT(t) }

type mockTiDBPullerSuite struct {
}

var _ = check.Suite(&mockTiDBPullerSuite{})

func (s *mockTiDBPullerSuite) TestCanGetKVEntrys(c *check.C) {
	sqlChan := make(chan string)
	puller, err := NewMockTiDBPuller(c, sqlChan)
	c.Assert(err, check.IsNil)
	rawKvChan := puller.Pull()
	sqls := []string{
		"create table test.pkh(id int primary key, a int)",
		"create table test.pknh(id varchar(255) primary key, a int)",
		"insert into test.pkh(id, a) values(1, 2)",
		"insert into test.pknh(id, a) values('1', 2)",
		"insert into test.pkh(id, a) values(2, 3)",
		"insert into test.pknh(id, a) values('2', 3)",
		"delete from test.pkh where id = 1",
		"delete from test.pknh where id = '1'",
		"update test.pkh set id = 1 where id = 2",
		"update test.pknh set id = 1 where id = 2",
	}
	go func() {
		for _, sql := range sqls {
			sqlChan <- sql
			time.Sleep(300 * time.Millisecond)
		}
		time.Sleep(1 * time.Second)
		puller.Close()
	}()
	var rawKvs []RawKVEntry
	for rawKv := range rawKvChan {
		rawKvs = append(rawKvs, rawKv)
		kvEntry, err := Unmarshal(&rawKv)
		c.Assert(err, check.IsNil)
		if e, ok := kvEntry.(*UnknownKVEntry); ok {
			fmt.Printf("key: %s, value: %s, op: %d\n", string(e.Key), string(e.Value), e.OpType)
		} else {
			log.Info("kv entry", zap.Reflect("kvEntry", kvEntry))
		}
	}
	c.Assert(len(rawKvs), check.Greater, 0)
}
