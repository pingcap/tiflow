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
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestCloudStorageWriteEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	parentDir := t.TempDir()
	uri := fmt.Sprintf("file:///%s?flush-interval=2s", parentDir)
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)

	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink = &config.SinkConfig{
		Protocol: config.ProtocolOpen.String(),
	}

	errCh := make(chan error, 5)
	s, err := NewCloudStorageSink(ctx, sinkURI, replicaConfig, errCh)
	require.Nil(t, err)

	// assume we have a large transaction and it is splitted into 10 small transactions
	txns := make([]*eventsink.TxnCallbackableEvent, 0, 10)
	cnt := 0
	batch := 100
	for i := 0; i < 10; i++ {
		txn := &eventsink.TxnCallbackableEvent{
			Event: &model.SingleTableTxn{
				CommitTs:     100,
				Table:        &model.TableName{Schema: "test", Table: "table1"},
				TableVersion: 33,
			},
			Callback: func() {
				cnt += batch
			},
		}
		for j := 0; j < batch; j++ {
			row := &model.RowChangedEvent{
				CommitTs:         100,
				Table:            &model.TableName{Schema: "test", Table: "table1"},
				TableInfoVersion: 33,
				Columns: []*model.Column{
					{Name: "c1", Value: i*batch + j},
					{Name: "c2", Value: "hello world"},
				},
			}
			txn.Event.Rows = append(txn.Event.Rows, row)
		}
		txns = append(txns, txn)
	}
	tableDir := path.Join(parentDir, "test/table1/33")
	os.MkdirAll(tableDir, 0o755)
	err = s.WriteEvents(txns...)
	require.Nil(t, err)
	time.Sleep(4 * time.Second)

	files, err := os.ReadDir(tableDir)
	require.Nil(t, err)
	require.Len(t, files, 1)
	content, err := os.ReadFile(path.Join(tableDir, files[0].Name()))
	require.Nil(t, err)
	require.Greater(t, len(content), 0)

	require.Equal(t, 1000, cnt)
	cancel()
	err = s.Close()
	require.Nil(t, err)
}
