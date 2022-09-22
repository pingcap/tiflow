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
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/model"
	filter "github.com/pingcap/tidb/util/table-filter"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
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
					{Matcher: []string{"test.tbl3"}, DispatcherRule: "ts"},
					{Matcher: []string{"test.tbl4"}, DispatcherRule: "rowid"},
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

func TestVerifyAndComplete(t *testing.T) {
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

	err := info.VerifyAndComplete()
	require.Nil(t, err)
	require.Equal(t, SortUnified, info.Engine)

	marshalConfig1, err := info.Config.Marshal()
	require.Nil(t, err)
	defaultConfig := config.GetDefaultReplicaConfig()
	marshalConfig2, err := defaultConfig.Marshal()
	require.Nil(t, err)
	require.Equal(t, marshalConfig2, marshalConfig1)
}

func TestFixStateIncompatible(t *testing.T) {
	t.Parallel()

	// Test to fix incompatible states.
	testCases := []struct {
		info          *ChangeFeedInfo
		expectedState FeedState
	}{
		{
			info: &ChangeFeedInfo{
				AdminJobType:   AdminStop,
				State:          StateNormal,
				Error:          nil,
				CreatorVersion: "",
				SinkURI:        "mysql://root:test@127.0.0.1:3306/",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: config.ProtocolDefault.String()},
				},
			},
			expectedState: StateStopped,
		},
		{
			info: &ChangeFeedInfo{
				AdminJobType:   AdminStop,
				State:          StateNormal,
				Error:          nil,
				CreatorVersion: "4.0.14",
				SinkURI:        "mysql://root:test@127.0.0.1:3306/",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: config.ProtocolDefault.String()},
				},
			},
			expectedState: StateStopped,
		},
		{
			info: &ChangeFeedInfo{
				AdminJobType:   AdminStop,
				State:          StateNormal,
				Error:          nil,
				CreatorVersion: "5.0.5",
				SinkURI:        "mysql://root:test@127.0.0.1:3306/",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: config.ProtocolDefault.String()},
				},
			},
			expectedState: StateStopped,
		},
	}

	for _, tc := range testCases {
		tc.info.FixIncompatible()
		require.Equal(t, tc.expectedState, tc.info.State)
	}
}

func TestFixSinkProtocolIncompatible(t *testing.T) {
	t.Parallel()

	emptyProtocolStr := ""
	// Test to fix incompatible protocols.
	configTestCases := []struct {
		info                *ChangeFeedInfo
		expectedProtocol    config.Protocol
		expectedProtocolStr string
	}{
		{
			info: &ChangeFeedInfo{
				AdminJobType:   AdminStop,
				State:          StateStopped,
				Error:          nil,
				CreatorVersion: "",
				SinkURI:        "kafka://127.0.0.1:9092/ticdc-test2",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: config.ProtocolAvro.String()},
				},
			},
			expectedProtocol: config.ProtocolAvro,
		},
		{
			info: &ChangeFeedInfo{
				AdminJobType:   AdminStop,
				State:          StateStopped,
				Error:          nil,
				CreatorVersion: "",
				SinkURI:        "kafka://127.0.0.1:9092/ticdc-test2",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: config.ProtocolDefault.String()},
				},
			},
			expectedProtocol: config.ProtocolOpen,
		},
		{
			info: &ChangeFeedInfo{
				AdminJobType:   AdminStop,
				State:          StateStopped,
				Error:          nil,
				CreatorVersion: "",
				SinkURI:        "kafka://127.0.0.1:9092/ticdc-test2",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: "random"},
				},
			},
			expectedProtocol: config.ProtocolOpen,
		},
		{
			info: &ChangeFeedInfo{
				AdminJobType:   AdminStop,
				State:          StateStopped,
				Error:          nil,
				CreatorVersion: "5.3.0",
				SinkURI:        "kafka://127.0.0.1:9092/ticdc-test2",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: config.ProtocolDefault.String()},
				},
			},
			expectedProtocol: config.ProtocolOpen,
		},
		{
			info: &ChangeFeedInfo{
				AdminJobType:   AdminStop,
				State:          StateStopped,
				Error:          nil,
				CreatorVersion: "5.3.0",
				SinkURI:        "kafka://127.0.0.1:9092/ticdc-test2",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: "random"},
				},
			},
			expectedProtocol: config.ProtocolOpen,
		},
		{
			info: &ChangeFeedInfo{
				AdminJobType:   AdminStop,
				State:          StateStopped,
				Error:          nil,
				CreatorVersion: "5.3.0",
				SinkURI:        "mysql://127.0.0.1:9092/ticdc-test2",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: "default"},
				},
			},
			expectedProtocolStr: emptyProtocolStr,
		},
		{
			info: &ChangeFeedInfo{
				AdminJobType:   AdminStop,
				State:          StateStopped,
				Error:          nil,
				CreatorVersion: "5.3.0",
				SinkURI:        "tidb://127.0.0.1:9092/ticdc-test2",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: "random"},
				},
			},
			expectedProtocolStr: emptyProtocolStr,
		},
	}

	for _, tc := range configTestCases {
		tc.info.FixIncompatible()
		if tc.expectedProtocolStr != "" {
			require.Equal(t, tc.expectedProtocolStr, tc.info.Config.Sink.Protocol)
		} else {
			var protocol config.Protocol
			err := protocol.FromString(tc.info.Config.Sink.Protocol)
			if strings.Contains(tc.info.SinkURI, "kafka") {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), "ErrMQSinkUnknownProtocol")
			}
		}
	}

	sinkURITestCases := []struct {
		info                *ChangeFeedInfo
		expectedSinkURI     string
		expectedProtocolStr *string
	}{
		{
			info: &ChangeFeedInfo{
				AdminJobType:   AdminStop,
				State:          StateStopped,
				Error:          nil,
				CreatorVersion: "",
				SinkURI:        "kafka://127.0.0.1:9092/ticdc-test2?protocol=canal",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: config.ProtocolDefault.String()},
				},
			},
			expectedSinkURI: "kafka://127.0.0.1:9092/ticdc-test2?protocol=canal",
		},
		{
			info: &ChangeFeedInfo{
				AdminJobType:   AdminStop,
				State:          StateStopped,
				Error:          nil,
				CreatorVersion: "",
				SinkURI:        "kafka://127.0.0.1:9092/ticdc-test2?protocol=random",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: config.ProtocolDefault.String()},
				},
			},
			expectedSinkURI: "kafka://127.0.0.1:9092/ticdc-test2?protocol=open-protocol",
		},
		{
			info: &ChangeFeedInfo{
				AdminJobType:   AdminStop,
				State:          StateStopped,
				Error:          nil,
				CreatorVersion: "5.3.0",
				SinkURI:        "kafka://127.0.0.1:9092/ticdc-test2?protocol=canal",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: config.ProtocolDefault.String()},
				},
			},
			expectedSinkURI: "kafka://127.0.0.1:9092/ticdc-test2?protocol=canal",
		},
		{
			info: &ChangeFeedInfo{
				AdminJobType:   AdminStop,
				State:          StateStopped,
				Error:          nil,
				CreatorVersion: "5.3.0",
				SinkURI:        "kafka://127.0.0.1:9092/ticdc-test2?protocol=random",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: config.ProtocolDefault.String()},
				},
			},
			expectedSinkURI: "kafka://127.0.0.1:9092/ticdc-test2?protocol=open-protocol",
		},
		{
			info: &ChangeFeedInfo{
				AdminJobType:   AdminStop,
				State:          StateStopped,
				Error:          nil,
				CreatorVersion: "5.3.0",
				SinkURI:        "mysql://127.0.0.1:9092/ticdc-test2?protocol=random",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: config.ProtocolDefault.String()},
				},
			},
			expectedSinkURI:     "mysql://127.0.0.1:9092/ticdc-test2",
			expectedProtocolStr: &emptyProtocolStr,
		},
		{
			info: &ChangeFeedInfo{
				AdminJobType:   AdminStop,
				State:          StateStopped,
				Error:          nil,
				CreatorVersion: "5.3.0",
				SinkURI:        "mysql://127.0.0.1:9092/ticdc-test2?protocol=default",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: config.ProtocolAvro.String()},
				},
			},
			expectedSinkURI:     "mysql://127.0.0.1:9092/ticdc-test2",
			expectedProtocolStr: &emptyProtocolStr,
		},
	}

	for _, tc := range sinkURITestCases {
		tc.info.FixIncompatible()
		require.Equal(t, tc.expectedSinkURI, tc.info.SinkURI)
		if tc.expectedProtocolStr != nil {
			require.Equal(t, *tc.expectedProtocolStr, tc.info.Config.Sink.Protocol)
		}
	}
}

func TestFixState(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		info          *ChangeFeedInfo
		expectedState FeedState
	}{
		{
			info: &ChangeFeedInfo{
				AdminJobType: AdminNone,
				State:        StateNormal,
				Error:        nil,
			},
			expectedState: StateNormal,
		},
		{
			info: &ChangeFeedInfo{
				AdminJobType: AdminResume,
				State:        StateNormal,
				Error:        nil,
			},
			expectedState: StateNormal,
		},
		{
			info: &ChangeFeedInfo{
				AdminJobType: AdminNone,
				State:        StateNormal,
				Error: &RunningError{
					Code: string(cerrors.ErrGCTTLExceeded.RFCCode()),
				},
			},
			expectedState: StateFailed,
		},
		{
			info: &ChangeFeedInfo{
				AdminJobType: AdminResume,
				State:        StateNormal,
				Error: &RunningError{
					Code: string(cerrors.ErrGCTTLExceeded.RFCCode()),
				},
			},
			expectedState: StateFailed,
		},
		{
			info: &ChangeFeedInfo{
				AdminJobType: AdminNone,
				State:        StateNormal,
				Error: &RunningError{
					Code: string(cerrors.ErrClusterIDMismatch.RFCCode()),
				},
			},
			expectedState: StateError,
		},
		{
			info: &ChangeFeedInfo{
				AdminJobType: AdminResume,
				State:        StateNormal,
				Error: &RunningError{
					Code: string(cerrors.ErrClusterIDMismatch.RFCCode()),
				},
			},
			expectedState: StateError,
		},
		{
			info: &ChangeFeedInfo{
				AdminJobType: AdminStop,
				State:        StateNormal,
				Error:        nil,
			},
			expectedState: StateStopped,
		},
		{
			info: &ChangeFeedInfo{
				AdminJobType: AdminFinish,
				State:        StateNormal,
				Error:        nil,
			},
			expectedState: StateFinished,
		},
		{
			info: &ChangeFeedInfo{
				AdminJobType: AdminRemove,
				State:        StateNormal,
				Error:        nil,
			},
			expectedState: StateRemoved,
		},
		{
			info: &ChangeFeedInfo{
				AdminJobType: AdminRemove,
				State:        StateNormal,
				Error:        nil,
			},
			expectedState: StateRemoved,
		},
	}

	for _, tc := range testCases {
		tc.info.fixState()
		require.Equal(t, tc.expectedState, tc.info.State)
	}
}

func TestFixMysqlSinkProtocol(t *testing.T) {
	t.Parallel()
	// Test fixing the protocol in the configuration.
	configTestCases := []struct {
		info             *ChangeFeedInfo
		expectedProtocol string
	}{
		{
			info: &ChangeFeedInfo{
				SinkURI: "mysql://root:test@127.0.0.1:3306/",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: config.ProtocolDefault.String()},
				},
			},
			expectedProtocol: "",
		},
		{
			info: &ChangeFeedInfo{
				SinkURI: "mysql://root:test@127.0.0.1:3306/",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: "whatever"},
				},
			},
			expectedProtocol: "",
		},
	}

	for _, tc := range configTestCases {
		tc.info.fixMySQLSinkProtocol()
		require.Equal(t, tc.expectedProtocol, tc.info.Config.Sink.Protocol)
	}

	sinkURITestCases := []struct {
		info            *ChangeFeedInfo
		expectedSinkURI string
	}{
		{
			info: &ChangeFeedInfo{
				SinkURI: "mysql://root:test@127.0.0.1:3306/?protocol=open-protocol",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: config.ProtocolDefault.String()},
				},
			},
			expectedSinkURI: "mysql://root:test@127.0.0.1:3306/",
		},
		{
			info: &ChangeFeedInfo{
				SinkURI: "mysql://root:test@127.0.0.1:3306/?protocol=default",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: ""},
				},
			},
			expectedSinkURI: "mysql://root:test@127.0.0.1:3306/",
		},
	}

	for _, tc := range sinkURITestCases {
		tc.info.fixMySQLSinkProtocol()
		require.Equal(t, tc.expectedSinkURI, tc.info.SinkURI)
	}
}

func TestFixMQSinkProtocol(t *testing.T) {
	t.Parallel()

	// Test fixing the protocol in the configuration.
	configTestCases := []struct {
		info             *ChangeFeedInfo
		expectedProtocol config.Protocol
	}{
		{
			info: &ChangeFeedInfo{
				SinkURI: "kafka://127.0.0.1:9092/ticdc-test2",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: config.ProtocolCanal.String()},
				},
			},
			expectedProtocol: config.ProtocolCanal,
		},
		{
			info: &ChangeFeedInfo{
				SinkURI: "kafka://127.0.0.1:9092/ticdc-test2",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: config.ProtocolDefault.String()},
				},
			},
			expectedProtocol: config.ProtocolOpen,
		},
		{
			info: &ChangeFeedInfo{
				SinkURI: "kafka://127.0.0.1:9092/ticdc-test2",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: "random"},
				},
			},
			expectedProtocol: config.ProtocolOpen,
		},
	}

	for _, tc := range configTestCases {
		tc.info.fixMQSinkProtocol()
		var protocol config.Protocol
		err := protocol.FromString(tc.info.Config.Sink.Protocol)
		require.Nil(t, err)
		require.Equal(t, tc.expectedProtocol, protocol)
	}

	// Test fixing the protocol in SinkURI.
	sinkURITestCases := []struct {
		info            *ChangeFeedInfo
		expectedSinkURI string
	}{
		{
			info: &ChangeFeedInfo{
				SinkURI: "kafka://127.0.0.1:9092/ticdc-test2",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: config.ProtocolCanal.String()},
				},
			},
			expectedSinkURI: "kafka://127.0.0.1:9092/ticdc-test2",
		},
		{
			info: &ChangeFeedInfo{
				SinkURI: "kafka://127.0.0.1:9092/ticdc-test2?protocol=canal",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: config.ProtocolDefault.String()},
				},
			},
			expectedSinkURI: "kafka://127.0.0.1:9092/ticdc-test2?protocol=canal",
		},
		{
			info: &ChangeFeedInfo{
				SinkURI: "kafka://127.0.0.1:9092/ticdc-test2?protocol=random",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: config.ProtocolDefault.String()},
				},
			},
			expectedSinkURI: "kafka://127.0.0.1:9092/ticdc-test2?protocol=open-protocol",
		},
		{
			info: &ChangeFeedInfo{
				SinkURI: "kafka://127.0.0.1:9092/ticdc-test2?protocol=random&max-message-size=15",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: config.ProtocolDefault.String()},
				},
			},
			expectedSinkURI: "kafka://127.0.0.1:9092/ticdc-test2?max-message-size=15&protocol=open-protocol",
		},
		{
			info: &ChangeFeedInfo{
				SinkURI: "kafka://127.0.0.1:9092/ticdc-test2?protocol=default&max-message-size=15",
				Config: &config.ReplicaConfig{
					Sink: &config.SinkConfig{Protocol: config.ProtocolDefault.String()},
				},
			},
			expectedSinkURI: "kafka://127.0.0.1:9092/ticdc-test2?max-message-size=15&protocol=open-protocol",
		},
	}

	for _, tc := range sinkURITestCases {
		tc.info.fixMQSinkProtocol()
		require.Equal(t, tc.expectedSinkURI, tc.info.SinkURI)
	}
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

func TestChangefeedInfoStringer(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		info                  *ChangeFeedInfo
		expectedSinkURIRegexp string
	}{
		{
			&ChangeFeedInfo{
				SinkURI: "blackhole://",
				StartTs: 418881574869139457,
			},
			`.*blackhole:.*`,
		},
		{
			&ChangeFeedInfo{
				SinkURI: "kafka://127.0.0.1:9092/ticdc-test2",
				StartTs: 418881574869139457,
			},
			`.*kafka://.*ticdc-test2.*`,
		},
		{
			&ChangeFeedInfo{
				SinkURI: "mysql://root:124567@127.0.0.1:3306/",
				StartTs: 418881574869139457,
			},
			`.*mysql://root:xxxxx@127.0.0.1:3306.*`,
		},
		{
			&ChangeFeedInfo{
				SinkURI: "mysql://root@127.0.0.1:3306/",
				StartTs: 418881574869139457,
			},
			`.*mysql://root@127.0.0.1:3306.*`,
		},
		{
			&ChangeFeedInfo{
				SinkURI: "mysql://root:test%21%23%24%25%5E%26%2A@127.0.0.1:3306/",
				StartTs: 418881574869139457,
			},
			`.*mysql://root:xxxxx@127.0.0.1:3306/.*`,
		},
	}

	for _, tc := range testcases {
		require.Regexp(t, tc.expectedSinkURIRegexp, tc.info.String())
	}
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
