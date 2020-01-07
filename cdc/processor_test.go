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
	"context"
	"sort"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/pingcap/check"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/roles/storage"
	"github.com/pingcap/ticdc/cdc/schema"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/etcd"
)

type processorSuite struct{}

type mockTsRWriter struct {
	l                sync.Mutex
	globalResolvedTs uint64

	memInfo     *model.TaskStatus
	storageInfo *model.TaskStatus
}

var _ storage.ProcessorTsRWriter = &mockTsRWriter{}

// ReadGlobalResolvedTs implement ProcessorTsRWriter interface.
func (s *mockTsRWriter) ReadGlobalResolvedTs(ctx context.Context) (uint64, error) {
	s.l.Lock()
	defer s.l.Unlock()
	return s.globalResolvedTs, nil
}

// GetTaskStatus implement ProcessorTsRWriter interface.
func (s *mockTsRWriter) GetTaskStatus() *model.TaskStatus {
	return s.memInfo
}

// WriteInfoIntoStorage implement ProcessorTsRWriter interface.
func (s *mockTsRWriter) WriteInfoIntoStorage(ctx context.Context) error {
	s.storageInfo = s.memInfo.Clone()
	return nil
}

// UpdateInfo implement ProcessorTsRWriter interface.
func (s *mockTsRWriter) UpdateInfo(ctx context.Context) (oldInfo *model.TaskStatus, newInfo *model.TaskStatus, err error) {
	oldInfo = s.memInfo
	newInfo = s.storageInfo

	s.memInfo = s.storageInfo
	s.storageInfo = s.memInfo.Clone()

	return
}

func (s *mockTsRWriter) SetGlobalResolvedTs(ts uint64) {
	s.l.Lock()
	defer s.l.Unlock()
	s.globalResolvedTs = ts
}

// mockMounter pretend to decode a RawTxn by returning a Txn of the same Ts
type mockMounter struct{}

func (m mockMounter) Mount(rawTxn model.RawTxn) (model.Txn, error) {
	return model.Txn{Ts: rawTxn.Ts}, nil
}

// mockSinker append all received Txns for validation
type mockSinker struct {
	sink.Sink
	synced []model.Txn
	mu     sync.Mutex
}

func (m *mockSinker) Emit(ctx context.Context, txns ...model.Txn) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.synced = append(m.synced, txns...)
	return nil
}

var _ = check.Suite(&processorSuite{})

type processorTestCase struct {
	rawTxnTs         [][]uint64
	globalResolvedTs []uint64
	expect           [][]uint64
}

func (p *processorSuite) TestProcessor(c *check.C) {
	c.Skip("can't create mock puller")
	cases := &processorTestCase{
		rawTxnTs: [][]uint64{
			{1, 4, 7, 9, 12, 14, 16, 20},
			{2, 4, 8, 13, 24},
		},
		globalResolvedTs: []uint64{14, 15, 19},
		expect: [][]uint64{
			{1, 2, 4, 4, 7, 8, 9, 12, 13, 14},
			{},
			{16},
		},
	}
	runCase(c, cases)
}

func runCase(c *check.C, cases *processorTestCase) {
	origFSchema := fCreateSchema
	fCreateSchema = func(pdEndpoints []string) (*schema.Storage, error) {
		return nil, nil
	}
	origFNewPD := fNewPDCli
	fNewPDCli = func(pdAddrs []string, security pd.SecurityOption) (pd.Client, error) {
		return nil, nil
	}
	origFNewTsRw := fNewTsRWriter
	fNewTsRWriter = func(cli *clientv3.Client, changefeedID, captureID string) (storage.ProcessorTsRWriter, error) {
		return &mockTsRWriter{}, nil
	}
	origFNewMounter := fNewMounter
	fNewMounter = func(schema *schema.Storage) mounter {
		return mockMounter{}
	}
	origFNewSink := fNewMySQLSink
	sinker := &mockSinker{}
	fNewMySQLSink = func(sinkURI string, infoGetter sink.TableInfoGetter, opts map[string]string) (sink.Sink, error) {
		return sinker, nil
	}
	defer func() {
		fCreateSchema = origFSchema
		fNewPDCli = origFNewPD
		fNewTsRWriter = origFNewTsRw
		fNewMounter = origFNewMounter
		fNewMySQLSink = origFNewSink
	}()

	dir := c.MkDir()
	etcdURL, etcd, err := etcd.SetupEmbedEtcd(dir)
	c.Assert(err, check.IsNil)
	defer etcd.Close()

	p, err := NewProcessor([]string{etcdURL.String()}, model.ChangeFeedDetail{}, "", "")
	c.Assert(err, check.IsNil)
	errCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	p.Run(ctx, errCh)

	for i, rawTxnTs := range cases.rawTxnTs {
		p.addTable(ctx, int64(i), 0)

		table := p.tables[int64(i)]
		input := table.inputTxn

		go func(rawTxnTs []uint64) {
			for _, txnTs := range rawTxnTs {
				input <- model.RawTxn{Ts: txnTs}
			}
		}(rawTxnTs)
	}

	for i, globalResolvedTs := range cases.globalResolvedTs {
		// hack to simulate owner to update global resolved ts
		p.getTsRwriter().(*mockTsRWriter).SetGlobalResolvedTs(globalResolvedTs)
		// waiting for processor push to resolvedTs
		for {
			sinker.mu.Lock()
			needBreak := len(sinker.synced) == len(cases.expect[i])
			sinker.mu.Unlock()
			if needBreak {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}

		sinker.mu.Lock()
		syncedTs := make([]uint64, 0, len(sinker.synced))
		for _, s := range sinker.synced {
			syncedTs = append(syncedTs, s.Ts)
		}
		sort.Slice(syncedTs, func(i, j int) bool {
			return syncedTs[i] < syncedTs[j]
		})
		c.Assert(syncedTs, check.DeepEquals, cases.expect[i])
		sinker.synced = sinker.synced[:0]
		sinker.mu.Unlock()
	}
	cancel()
}

func (p *processorSuite) TestDiffProcessTableInfos(c *check.C) {
	infos := make([]*model.ProcessTableInfo, 0, 3)
	for i := uint64(0); i < uint64(3); i++ {
		infos = append(infos, &model.ProcessTableInfo{ID: i, StartTs: 10 * i})
	}
	var (
		emptyInfo = make([]*model.ProcessTableInfo, 0)
		cases     = []struct {
			oldInfo []*model.ProcessTableInfo
			newInfo []*model.ProcessTableInfo
			removed []*model.ProcessTableInfo
			added   []*model.ProcessTableInfo
		}{
			{emptyInfo, emptyInfo, nil, nil},
			{[]*model.ProcessTableInfo{infos[0]}, []*model.ProcessTableInfo{infos[0]}, nil, nil},
			{emptyInfo, []*model.ProcessTableInfo{infos[0]}, nil, []*model.ProcessTableInfo{infos[0]}},
			{[]*model.ProcessTableInfo{infos[0]}, emptyInfo, []*model.ProcessTableInfo{infos[0]}, nil},
			{[]*model.ProcessTableInfo{infos[0]}, []*model.ProcessTableInfo{infos[1]}, []*model.ProcessTableInfo{infos[0]}, []*model.ProcessTableInfo{infos[1]}},
			{[]*model.ProcessTableInfo{infos[1], infos[0]}, []*model.ProcessTableInfo{infos[2], infos[1]}, []*model.ProcessTableInfo{infos[0]}, []*model.ProcessTableInfo{infos[2]}},
			{[]*model.ProcessTableInfo{infos[1]}, []*model.ProcessTableInfo{infos[0]}, []*model.ProcessTableInfo{infos[1]}, []*model.ProcessTableInfo{infos[0]}},
		}
	)

	for _, tc := range cases {
		removed, added := diffProcessTableInfos(tc.oldInfo, tc.newInfo)
		c.Assert(removed, check.DeepEquals, tc.removed)
		c.Assert(added, check.DeepEquals, tc.added)
	}
}

type txnChannelSuite struct{}

var _ = check.Suite(&txnChannelSuite{})

func (s *txnChannelSuite) TestShouldForwardTxnsByTs(c *check.C) {
	input := make(chan model.RawTxn, 5)
	var lastTs uint64
	callback := func(ts uint64) {
		lastTs = ts
	}
	tc := newTxnChannel(input, 5, callback)
	for _, ts := range []uint64{1, 2, 4, 6} {
		select {
		case input <- model.RawTxn{Ts: ts}:
		case <-time.After(time.Second):
			c.Fatal("Timeout sending to input")
		}
	}
	close(input)

	output := make(chan model.RawTxn, 5)

	assertCorrectOutput := func(expected []uint64) {
		for _, ts := range expected {
			c.Logf("Checking %d", ts)
			select {
			case e := <-output:
				c.Assert(e.Ts, check.Equals, ts)
			case <-time.After(time.Second):
				c.Fatal("Timeout reading output")
			}
		}

		select {
		case <-output:
			c.Fatal("Output should be empty now")
		default:
		}
	}

	tc.Forward(context.Background(), 3, output)
	// Assert that all txns with ts not greater than 3 is sent to output
	assertCorrectOutput([]uint64{1, 2})
	tc.Forward(context.Background(), 10, output)
	// Assert that all txns with ts not greater than 10 is sent to output
	assertCorrectOutput([]uint64{4, 6})
	c.Assert(lastTs, check.Equals, uint64(6))
}

func (s *txnChannelSuite) TestShouldBeCancellable(c *check.C) {
	input := make(chan model.RawTxn, 5)
	tc := newTxnChannel(input, 5, func(ts uint64) {})
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		tc.Forward(ctx, 1, make(chan model.RawTxn))
		close(stopped)
	}()
	cancel()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		c.Fatal("Not stopped in time after cancelled")
	}
}
