// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package cloudstorage

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"sync/atomic"
	"testing"
	"time"

	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	"github.com/pingcap/tiflow/cdc/sink/tablesink/state"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func setClock(s *DMLSink, clock clock.Clock) {
	for _, w := range s.workers {
		w.filePathGenerator.SetClock(clock)
	}
}

func generateTxnEvents(
	cnt *uint64,
	batch int,
	tableStatus *state.TableSinkState,
) []*dmlsink.TxnCallbackableEvent {
	// assume we have a large transaction and it is splitted into 10 small transactions
	txns := make([]*dmlsink.TxnCallbackableEvent, 0, 10)

	for i := 0; i < 10; i++ {
		txn := &dmlsink.TxnCallbackableEvent{
			Event: &model.SingleTableTxn{
				CommitTs:         100,
				Table:            &model.TableName{Schema: "test", Table: "table1"},
				TableInfoVersion: 33,
				TableInfo: &model.TableInfo{
					TableName: model.TableName{
						Schema: "test", Table: "table1",
					},
					Version: 33,
					TableInfo: &timodel.TableInfo{
						Columns: []*timodel.ColumnInfo{
							{ID: 1, Name: timodel.NewCIStr("c1"), FieldType: *types.NewFieldType(mysql.TypeLong)},
							{ID: 2, Name: timodel.NewCIStr("c2"), FieldType: *types.NewFieldType(mysql.TypeVarchar)},
						},
					},
				},
			},
			Callback: func() {
				atomic.AddUint64(cnt, uint64(batch))
			},
			SinkState: tableStatus,
		}
		for j := 0; j < batch; j++ {
			row := &model.RowChangedEvent{
				CommitTs:  100,
				Table:     &model.TableName{Schema: "test", Table: "table1"},
				TableInfo: &model.TableInfo{TableName: model.TableName{Schema: "test", Table: "table1"}, Version: 33},
				Columns: []*model.Column{
					{Name: "c1", Value: i*batch + j},
					{Name: "c2", Value: "hello world"},
				},
				ColInfos: []rowcodec.ColInfo{
					{ID: 1, Ft: types.NewFieldType(mysql.TypeLong)},
					{ID: 2, Ft: types.NewFieldType(mysql.TypeVarchar)},
				},
			}
			txn.Event.Rows = append(txn.Event.Rows, row)
		}
		txns = append(txns, txn)
	}

	return txns
}

func TestCloudStorageWriteEventsWithoutDateSeparator(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	parentDir := t.TempDir()
	uri := fmt.Sprintf("file:///%s?flush-interval=2s", parentDir)
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.Protocol = config.ProtocolCsv.String()
	replicaConfig.Sink.FileIndexWidth = 6
	errCh := make(chan error, 5)
	s, err := NewDMLSink(ctx, sinkURI, replicaConfig, errCh)
	require.Nil(t, err)
	var cnt uint64 = 0
	batch := 100
	tableStatus := state.TableSinkSinking

	// generating one dml file.
	txns := generateTxnEvents(&cnt, batch, &tableStatus)
	err = s.WriteEvents(txns...)
	require.Nil(t, err)
	time.Sleep(3 * time.Second)

	metaDir := path.Join(parentDir, "test/table1/meta")
	files, err := os.ReadDir(metaDir)
	require.Nil(t, err)
	require.Len(t, files, 1)

	tableDir := path.Join(parentDir, "test/table1/33")
	files, err = os.ReadDir(tableDir)
	require.Nil(t, err)
	require.Len(t, files, 2)
	var fileNames []string
	for _, f := range files {
		fileNames = append(fileNames, f.Name())
	}
	require.ElementsMatch(t, []string{"CDC000001.csv", "CDC.index"}, fileNames)
	content, err := os.ReadFile(path.Join(tableDir, "CDC000001.csv"))
	require.Nil(t, err)
	require.Greater(t, len(content), 0)

	content, err = os.ReadFile(path.Join(tableDir, "CDC.index"))
	require.Nil(t, err)
	require.Equal(t, "CDC000001.csv\n", string(content))
	require.Equal(t, uint64(1000), atomic.LoadUint64(&cnt))

	// generating another dml file.
	err = s.WriteEvents(txns...)
	require.Nil(t, err)
	time.Sleep(3 * time.Second)

	files, err = os.ReadDir(tableDir)
	require.Nil(t, err)
	require.Len(t, files, 3)
	fileNames = nil
	for _, f := range files {
		fileNames = append(fileNames, f.Name())
	}
	require.ElementsMatch(t, []string{
		"CDC000001.csv", "CDC000002.csv", "CDC.index",
	}, fileNames)
	content, err = os.ReadFile(path.Join(tableDir, "CDC000002.csv"))
	require.Nil(t, err)
	require.Greater(t, len(content), 0)

	content, err = os.ReadFile(path.Join(tableDir, "CDC.index"))
	require.Nil(t, err)
	require.Equal(t, "CDC000002.csv\n", string(content))
	require.Equal(t, uint64(2000), atomic.LoadUint64(&cnt))

	cancel()
	s.Close()
}

func TestCloudStorageWriteEventsWithDateSeparator(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	parentDir := t.TempDir()
	uri := fmt.Sprintf("file:///%s?flush-interval=2s", parentDir)
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.Protocol = config.ProtocolCsv.String()
	replicaConfig.Sink.DateSeparator = config.DateSeparatorDay.String()
	replicaConfig.Sink.FileIndexWidth = 6

	errCh := make(chan error, 5)
	s, err := NewDMLSink(ctx, sinkURI, replicaConfig, errCh)
	require.Nil(t, err)
	mockClock := clock.NewMock()
	setClock(s, mockClock)

	var cnt uint64 = 0
	batch := 100
	tableStatus := state.TableSinkSinking

	mockClock.Set(time.Date(2023, 3, 8, 23, 59, 58, 0, time.UTC))
	txns := generateTxnEvents(&cnt, batch, &tableStatus)
	tableDir := path.Join(parentDir, "test/table1/33/2023-03-08")
	err = s.WriteEvents(txns...)
	require.Nil(t, err)
	time.Sleep(3 * time.Second)

	files, err := os.ReadDir(tableDir)
	require.Nil(t, err)
	require.Len(t, files, 2)
	var fileNames []string
	for _, f := range files {
		fileNames = append(fileNames, f.Name())
	}
	require.ElementsMatch(t, []string{"CDC000001.csv", "CDC.index"}, fileNames)
	content, err := os.ReadFile(path.Join(tableDir, "CDC000001.csv"))
	require.Nil(t, err)
	require.Greater(t, len(content), 0)

	content, err = os.ReadFile(path.Join(tableDir, "CDC.index"))
	require.Nil(t, err)
	require.Equal(t, "CDC000001.csv\n", string(content))
	require.Equal(t, uint64(1000), atomic.LoadUint64(&cnt))

	// test date (day) is NOT changed.
	mockClock.Set(time.Date(2023, 3, 8, 23, 59, 59, 0, time.UTC))
	setClock(s, mockClock)

	err = s.WriteEvents(txns...)
	require.Nil(t, err)
	time.Sleep(3 * time.Second)

	files, err = os.ReadDir(tableDir)
	require.Nil(t, err)
	require.Len(t, files, 3)
	fileNames = nil
	for _, f := range files {
		fileNames = append(fileNames, f.Name())
	}
	require.ElementsMatch(t, []string{"CDC000001.csv", "CDC000002.csv", "CDC.index"}, fileNames)
	content, err = os.ReadFile(path.Join(tableDir, "CDC000002.csv"))
	require.Nil(t, err)
	require.Greater(t, len(content), 0)

	content, err = os.ReadFile(path.Join(tableDir, "CDC.index"))
	require.Nil(t, err)
	require.Equal(t, "CDC000002.csv\n", string(content))
	require.Equal(t, uint64(2000), atomic.LoadUint64(&cnt))

	// test date (day) is changed.
	mockClock.Set(time.Date(2023, 3, 9, 0, 0, 10, 0, time.UTC))
	setClock(s, mockClock)

	err = s.WriteEvents(txns...)
	require.Nil(t, err)
	time.Sleep(3 * time.Second)

	tableDir = path.Join(parentDir, "test/table1/33/2023-03-09")
	files, err = os.ReadDir(tableDir)
	require.Nil(t, err)
	require.Len(t, files, 2)
	fileNames = nil
	for _, f := range files {
		fileNames = append(fileNames, f.Name())
	}
	require.ElementsMatch(t, []string{"CDC000001.csv", "CDC.index"}, fileNames)
	content, err = os.ReadFile(path.Join(tableDir, "CDC000001.csv"))
	require.Nil(t, err)
	require.Greater(t, len(content), 0)

	content, err = os.ReadFile(path.Join(tableDir, "CDC.index"))
	require.Nil(t, err)
	require.Equal(t, "CDC000001.csv\n", string(content))
	require.Equal(t, uint64(3000), atomic.LoadUint64(&cnt))
	cancel()
	s.Close()

	// test table is scheduled from one node to another
	cnt = 0
	ctx, cancel = context.WithCancel(context.Background())
	s, err = NewDMLSink(ctx, sinkURI, replicaConfig, errCh)
	require.Nil(t, err)
	mockClock = clock.NewMock()
	mockClock.Set(time.Date(2023, 3, 9, 0, 1, 10, 0, time.UTC))
	setClock(s, mockClock)

	err = s.WriteEvents(txns...)
	require.Nil(t, err)
	time.Sleep(3 * time.Second)

	files, err = os.ReadDir(tableDir)
	require.Nil(t, err)
	require.Len(t, files, 3)
	fileNames = nil
	for _, f := range files {
		fileNames = append(fileNames, f.Name())
	}
	require.ElementsMatch(t, []string{"CDC000001.csv", "CDC000002.csv", "CDC.index"}, fileNames)
	content, err = os.ReadFile(path.Join(tableDir, "CDC000002.csv"))
	require.Nil(t, err)
	require.Greater(t, len(content), 0)

	content, err = os.ReadFile(path.Join(tableDir, "CDC.index"))
	require.Nil(t, err)
	require.Equal(t, "CDC000002.csv\n", string(content))
	require.Equal(t, uint64(1000), atomic.LoadUint64(&cnt))

	cancel()
	s.Close()
}
