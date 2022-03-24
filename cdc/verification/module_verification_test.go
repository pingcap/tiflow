//  Copyright 2022 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

package verification

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/db"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientV3 "go.etcd.io/etcd/client/v3"
)

func TestNewModuleVerification(t *testing.T) {
	_, err := NewModuleVerification(context.Background(), nil, nil)
	require.NotNil(t, err)

	_, err = NewModuleVerification(context.Background(), &ModuleVerificationConfig{}, nil)
	require.NotNil(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockEtcdCli := &etcd.MockEtcdClient{}
	mockEtcdCli.On("Watch", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	m1, err := NewModuleVerification(ctx, &ModuleVerificationConfig{ChangefeedID: "1"}, mockEtcdCli)
	require.Nil(t, err)
	m1.SentTrackData(ctx, Puller, []TrackData{{[]byte("1"), 1}})

	m2, err := NewModuleVerification(ctx, &ModuleVerificationConfig{ChangefeedID: "1"}, mockEtcdCli)
	require.Nil(t, err)
	require.Equal(t, m1, m2)

	ctx1, cancel1 := context.WithCancel(context.Background())
	m3, err := NewModuleVerification(ctx1, &ModuleVerificationConfig{ChangefeedID: "2"}, mockEtcdCli)
	require.Nil(t, err)
	cancel1()
	require.NotSame(t, m1, m3)
	time.Sleep(time.Millisecond * 3)

	m4, err := NewModuleVerification(ctx, &ModuleVerificationConfig{ChangefeedID: "2"}, mockEtcdCli)
	require.Nil(t, err)
	require.NotSame(t, m4, m3)

	err = m4.GC(ctx, "")
	require.Nil(t, err)

	ch := make(chan clientV3.WatchResponse, 1)
	task := &taskInfo{StartTs: "1", EndTs: "2"}
	v, err := task.Marshal()
	require.Nil(t, err)
	ch <- clientV3.WatchResponse{Events: []*clientV3.Event{{Kv: &mvccpb.KeyValue{Value: v}}}}
	close(ch)
	mockEtcdCli1 := &etcd.MockEtcdClient{}
	mockEtcdCli1.On("Watch", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(clientV3.WatchChan(ch))
	ctx2, cancel2 := context.WithCancel(context.Background())
	m, err := NewModuleVerification(ctx2, &ModuleVerificationConfig{ChangefeedID: "111"}, mockEtcdCli1)
	require.Nil(t, err)
	cancel2()
	time.Sleep(time.Millisecond * 5)
	err = m.Close()
	require.Nil(t, err)
	err = m.Close()
	require.Nil(t, err)
}

func TestModuleVerification_SentTrackData(t *testing.T) {
	mockBatch := &db.MockBatch{}
	mockBatch.On("Put", mock.Anything, mock.Anything)
	mockBatch.On("Repr").Return([]byte{})
	mockBatch.On("Commit").Return(errors.New("xxx"))
	mockBatch.On("Reset")

	cfg := &ModuleVerificationConfig{DBConfig: config.GetDefaultServerConfig().Debug.DB}
	cfg.DBConfig.BlockSize = 0
	m := &ModuleVerification{
		wb:  mockBatch,
		cfg: cfg,
	}
	m.SentTrackData(context.Background(), Puller, []TrackData{{TrackID: []byte("1"), CommitTs: 1}})
}

func TestModuleVerification_GC(t *testing.T) {
	cfg := config.GetDefaultServerConfig().Debug.DB
	type args struct {
		deleteCount  uint64
		nextDeleteTs time.Time
	}
	tests := []struct {
		name            string
		args            args
		dbErr           error
		wantDeleteCount uint64
		wantNextTs      time.Time
	}{
		{
			name: "delete count meet",
			args: args{
				deleteCount: uint64(cfg.CompactionDeletionThreshold),
			},
			wantDeleteCount: 0,
			wantNextTs:      time.Now().Add(time.Duration(cfg.CompactionPeriod) * time.Second),
		},
		{
			name: "time meet",
			args: args{
				deleteCount:  uint64(cfg.CompactionDeletionThreshold) - 1,
				nextDeleteTs: time.Now().Add(-time.Minute),
			},
			wantDeleteCount: 0,
			wantNextTs:      time.Now().Add(time.Duration(cfg.CompactionPeriod) * time.Second),
		},
		{
			name: "time meet && db err",
			args: args{
				deleteCount:  uint64(cfg.CompactionDeletionThreshold) - 1,
				nextDeleteTs: time.Now().Add(-time.Minute),
			},
			dbErr:           errors.New("xx"),
			wantDeleteCount: uint64(cfg.CompactionDeletionThreshold),
			wantNextTs:      time.Now().Add(-time.Minute),
		},
		{
			name: "not meet",
			args: args{
				deleteCount:  uint64(cfg.CompactionDeletionThreshold) - 2,
				nextDeleteTs: time.Now().Add(time.Minute),
			},
			wantDeleteCount: uint64(cfg.CompactionDeletionThreshold) - 1,
			wantNextTs:      time.Now().Add(time.Minute),
		},
	}

	for _, tt := range tests {
		mockDB := &db.MockDB{}
		mockDB.On("Compact", []byte{0x0}, bytes.Repeat([]byte{0xff}, 128)).Return(tt.dbErr)
		mockDB.On("DeleteRange", []byte{}, encodeKey(Sink, 11+1, nil)).Return(nil)

		m := &ModuleVerification{
			cfg:          &ModuleVerificationConfig{DBConfig: cfg},
			db:           mockDB,
			deleteCount:  tt.args.deleteCount,
			nextDeleteTs: tt.args.nextDeleteTs,
		}
		err := m.GC(context.Background(), "11")
		require.True(t, errors.ErrorEqual(err, tt.dbErr), tt.name)
		require.Equal(t, tt.wantDeleteCount, m.deleteCount, tt.name)
		require.GreaterOrEqual(t, m.nextDeleteTs.Unix(), tt.wantNextTs.Unix(), tt.name)
	}
}

func TestModuleVerification_Verify(t *testing.T) {
	cfg := config.GetDefaultServerConfig().Debug.DB
	cfg.Count = 1

	pebble, err := db.OpenPebble(context.Background(), db.Verification, 1, t.TempDir(), 0, cfg)
	require.Nil(t, err)
	wb := pebble.Batch(0)

	// cfg.BlockSize = 0
	m := &ModuleVerification{
		db:  pebble,
		wb:  wb,
		cfg: &ModuleVerificationConfig{CyclicEnable: true, DBConfig: cfg},
	}
	defer m.Close()

	// within [startTs,endTs] have the same data
	m.SentTrackData(context.Background(), Puller, []TrackData{{[]byte("2"), 2}, {[]byte("1"), 1}, {[]byte("1"), 3}})
	m.SentTrackData(context.Background(), Sink, []TrackData{{[]byte("1"), 1}, {[]byte("2"), 2}})
	ret := m.Verify(context.Background(), "1", "2")
	require.Nil(t, ret)
	ret = m.Verify(context.Background(), "", "2")
	require.Nil(t, ret)

	err = m.deleteTrackData("2")
	require.Nil(t, err)

	// within [startTs,endTs] have the different data
	m.SentTrackData(context.Background(), Puller, []TrackData{{[]byte("21"), 2}, {[]byte("1"), 1}, {[]byte("1"), 3}})
	m.SentTrackData(context.Background(), Sink, []TrackData{{[]byte("1"), 1}, {[]byte("2"), 2}})
	ret = m.Verify(context.Background(), "1", "2")
	require.NotNil(t, ret)
	err = m.deleteTrackData("2")
	require.Nil(t, err)

	// within [startTs,endTs] have the different data
	m.SentTrackData(context.Background(), Puller, []TrackData{{[]byte("21"), 2}, {[]byte("1"), 1}, {[]byte("1"), 3}})
	m.SentTrackData(context.Background(), Sorter, []TrackData{{[]byte("21"), 2}, {[]byte("1"), 1}, {[]byte("1"), 3}})
	m.SentTrackData(context.Background(), Sink, []TrackData{{[]byte("1"), 1}, {[]byte("2"), 2}})
	ret = m.Verify(context.Background(), "1", "2")
	require.NotNil(t, ret)
	err = m.deleteTrackData("2")
	require.Nil(t, err)

	// within [startTs,endTs] have the different data
	m.SentTrackData(context.Background(), Puller, []TrackData{{[]byte("21"), 2}, {[]byte("1"), 1}, {[]byte("1"), 3}})
	m.SentTrackData(context.Background(), Cyclic, []TrackData{{[]byte("21"), 2}, {[]byte("1"), 1}, {[]byte("1"), 3}})
	m.SentTrackData(context.Background(), Sink, []TrackData{{[]byte("1"), 431834667399774209}})

	ret = m.Verify(context.Background(), "1", "431834667399774209")
	t.Log(ret.Error())
	require.NotNil(t, ret)
}
