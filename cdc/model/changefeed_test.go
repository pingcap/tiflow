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

package model

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	filter "github.com/pingcap/tidb-tools/pkg/table-filter"
	"github.com/pingcap/tidb/parser/model"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestFillV1(t *testing.T) {
	t.Parallel()

	v1Config := `
{
    "sink-uri":"blackhole://",
    "opts":{

    },
    "start-ts":417136892416622595,
    "target-ts":0,
    "admin-job-type":0,
    "sort-engine":"memory",
    "sort-dir":".",
    "config":{
        "case-sensitive":true,
        "filter":{
            "do-tables":[
                {
                    "db-name":"test",
                    "tbl-name":"tbl1"
                },
                {
                    "db-name":"test",
                    "tbl-name":"tbl2"
                }
            ],
            "do-dbs":[
                "test1",
                "sys1"
            ],
            "ignore-tables":[
                {
                    "db-name":"test",
                    "tbl-name":"tbl3"
                },
                {
                    "db-name":"test",
                    "tbl-name":"tbl4"
                }
            ],
            "ignore-dbs":[
                "test",
                "sys"
            ],
            "ignore-txn-start-ts":[
                1,
                2
            ],
            "ddl-allow-list":"AQI="
        },
        "mounter":{
            "worker-num":64
        },
        "sink":{
            "dispatch-rules":[
                {
                    "db-name":"test",
                    "tbl-name":"tbl3",
                    "rule":"ts"
                },
                {
                    "db-name":"test",
                    "tbl-name":"tbl4",
                    "rule":"rowid"
                }
            ]
        },
        "cyclic-replication":{
            "enable":true,
            "replica-id":1,
            "filter-replica-ids":[
                2,
                3
            ],
            "id-buckets":4,
            "sync-ddl":true
        }
    }
}
`
	cfg := &ChangeFeedInfo{}
	err := cfg.Unmarshal([]byte(v1Config))
	require.Nil(t, err)
	require.Equal(t, &ChangeFeedInfo{
		SinkURI: "blackhole://",
		Opts: map[string]string{
			"_cyclic_relax_sql_mode": `{"enable":true,"replica-id":1,"filter-replica-ids":[2,3],"id-buckets":4,"sync-ddl":true}`,
		},
		StartTs: 417136892416622595,
		Engine:  "memory",
		SortDir: ".",
		Config: &config.ReplicaConfig{
			CaseSensitive: true,
			Filter: &config.FilterConfig{
				MySQLReplicationRules: &filter.MySQLReplicationRules{
					DoTables: []*filter.Table{{
						Schema: "test",
						Name:   "tbl1",
					}, {
						Schema: "test",
						Name:   "tbl2",
					}},
					DoDBs: []string{"test1", "sys1"},
					IgnoreTables: []*filter.Table{{
						Schema: "test",
						Name:   "tbl3",
					}, {
						Schema: "test",
						Name:   "tbl4",
					}},
					IgnoreDBs: []string{"test", "sys"},
				},
				IgnoreTxnStartTs: []uint64{1, 2},
				DDLAllowlist:     []model.ActionType{1, 2},
			},
			Mounter: &config.MounterConfig{
				WorkerNum: 64,
			},
			Sink: &config.SinkConfig{
				DispatchRules: []*config.DispatchRule{
					{Matcher: []string{"test.tbl3"}, Dispatcher: "ts"},
					{Matcher: []string{"test.tbl4"}, Dispatcher: "rowid"},
				},
			},
			Cyclic: &config.CyclicConfig{
				Enable:          true,
				ReplicaID:       1,
				FilterReplicaID: []uint64{2, 3},
				IDBuckets:       4,
				SyncDDL:         true,
			},
		},
	}, cfg)
}

func TestVerifyAndFix(t *testing.T) {
	t.Parallel()

	info := &ChangeFeedInfo{
		SinkURI: "blackhole://",
		Opts:    map[string]string{},
		StartTs: 417257993615179777,
		Config: &config.ReplicaConfig{
			CaseSensitive:    true,
			EnableOldValue:   true,
			CheckGCSafePoint: true,
		},
	}

	err := info.VerifyAndFix()
	require.Nil(t, err)
	require.Equal(t, SortUnified, info.Engine)

	marshalConfig1, err := info.Config.Marshal()
	require.Nil(t, err)
	defaultConfig := config.GetDefaultReplicaConfig()
	marshalConfig2, err := defaultConfig.Marshal()
	require.Nil(t, err)
	require.Equal(t, marshalConfig2, marshalConfig1)
}

func TestChangeFeedInfoClone(t *testing.T) {
	t.Parallel()

	info := &ChangeFeedInfo{
		SinkURI: "blackhole://",
		Opts:    map[string]string{},
		StartTs: 417257993615179777,
		Config: &config.ReplicaConfig{
			CaseSensitive:    true,
			EnableOldValue:   true,
			CheckGCSafePoint: true,
		},
	}

	cloned, err := info.Clone()
	require.Nil(t, err)
	sinkURI := "mysql://unix:/var/run/tidb.sock"
	cloned.SinkURI = sinkURI
	cloned.Config.EnableOldValue = false
	require.Equal(t, sinkURI, cloned.SinkURI)
	require.False(t, cloned.Config.EnableOldValue)
	require.Equal(t, "blackhole://", info.SinkURI)
	require.True(t, info.Config.EnableOldValue)
}

func TestCheckErrorHistory(t *testing.T) {
	t.Parallel()

	now := time.Now()
	info := &ChangeFeedInfo{
		ErrorHis: []int64{},
	}
	for i := 0; i < 5; i++ {
		tm := now.Add(-errorHistoryGCInterval)
		info.ErrorHis = append(info.ErrorHis, tm.UnixNano()/1e6)
		time.Sleep(time.Millisecond)
	}
	for i := 0; i < ErrorHistoryThreshold-1; i++ {
		info.ErrorHis = append(info.ErrorHis, time.Now().UnixNano()/1e6)
		time.Sleep(time.Millisecond)
	}
	time.Sleep(time.Millisecond)
	needSave, canInit := info.CheckErrorHistory()
	require.True(t, needSave)
	require.True(t, canInit)
	require.Equal(t, ErrorHistoryThreshold-1, len(info.ErrorHis))

	info.ErrorHis = append(info.ErrorHis, time.Now().UnixNano()/1e6)
	needSave, canInit = info.CheckErrorHistory()
	require.False(t, needSave)
	require.False(t, canInit)
}

func TestChangefeedInfoStringer(t *testing.T) {
	t.Parallel()

	info := &ChangeFeedInfo{
		SinkURI: "blackhole://",
		StartTs: 418881574869139457,
	}
	require.Regexp(t, "^.*sink-uri\":\"\\*\\*\\*\".*$", info.String())
}

func TestValidateChangefeedID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		id      string
		wantErr bool
	}{
		{
			name:    "alphabet",
			id:      "testTtTT",
			wantErr: false,
		},
		{
			name:    "number",
			id:      "01131323",
			wantErr: false,
		},
		{
			name:    "mixed",
			id:      "9ff52acaA-aea6-4022-8eVc4-fbee3fD2c7890",
			wantErr: false,
		},
		{
			name:    "len==128",
			id:      "1234567890-1234567890-1234567890-1234567890-1234567890-1234567890-1234567890-1234567890-1234567890123456789012345678901234567890",
			wantErr: false,
		},
		{
			name:    "empty string 1",
			id:      "",
			wantErr: true,
		},
		{
			name:    "empty string 2",
			id:      "   ",
			wantErr: true,
		},
		{
			name:    "test_task",
			id:      "test_task ",
			wantErr: true,
		},
		{
			name:    "job$",
			id:      "job$ ",
			wantErr: true,
		},
		{
			name:    "test-",
			id:      "test-",
			wantErr: true,
		},
		{
			name:    "-",
			id:      "-",
			wantErr: true,
		},
		{
			name:    "-sfsdfdf1",
			id:      "-sfsdfdf1",
			wantErr: true,
		},
		{
			name:    "len==129",
			id:      "1234567890-1234567890-1234567890-1234567890-1234567890-1234567890-1234567890-1234567890-1234567890-123456789012345678901234567890",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		err := ValidateChangefeedID(tt.id)
		if !tt.wantErr {
			require.Nil(t, err, fmt.Sprintf("case:%s", tt.name))
		} else {
			require.True(t, cerror.ErrInvalidChangefeedID.Equal(err), fmt.Sprintf("case:%s", tt.name))
		}
	}
}

func TestGetTs(t *testing.T) {
	t.Parallel()

	var (
		startTs      uint64 = 418881574869139457
		targetTs     uint64 = 420891571239139085
		checkpointTs uint64 = 420874357546418177
		createTime          = time.Now()
		info                = &ChangeFeedInfo{
			SinkURI:    "blackhole://",
			CreateTime: createTime,
		}
	)
	require.Equal(t, info.GetStartTs(), oracle.GoTimeToTS(createTime))
	info.StartTs = startTs
	require.Equal(t, info.GetStartTs(), startTs)

	require.Equal(t, info.GetTargetTs(), uint64(math.MaxUint64))
	info.TargetTs = targetTs
	require.Equal(t, info.GetTargetTs(), targetTs)

	require.Equal(t, info.GetCheckpointTs(nil), startTs)
	status := &ChangeFeedStatus{CheckpointTs: checkpointTs}
	require.Equal(t, info.GetCheckpointTs(status), checkpointTs)
}

func TestFeedStateResumable(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		state    FeedState
		expected bool
	}{
		{
			state:    StateFailed,
			expected: true,
		},
		{
			state:    StateError,
			expected: true,
		},
		{
			state:    StateFinished,
			expected: true,
		},
		{
			state:    StateStopped,
			expected: true,
		},
		{
			state:    StateRemoved,
			expected: false,
		},
		{
			state:    StateNormal,
			expected: false,
		},
	}

	for _, tc := range testCases {
		require.Equal(t, tc.state.Resumable(), tc.expected)
	}
}
