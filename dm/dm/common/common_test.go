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

package common

import (
	"net"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeyAdapter(t *testing.T) {
	testCases := []struct {
		keys    []string
		adapter KeyAdapter
		want    string
	}{
		{
			keys:    []string{"127.0.0.1:2382"},
			adapter: WorkerRegisterKeyAdapter,
			want:    "/dm-worker/r/3132372e302e302e313a32333832",
		},
		{
			keys:    []string{"worker1"},
			adapter: WorkerKeepAliveKeyAdapter,
			want:    "/dm-worker/a/776f726b657231",
		},
		{
			keys:    []string{"mysql1"},
			adapter: UpstreamConfigKeyAdapter,
			want:    "/dm-master/v2/upstream/config/6d7973716c31",
		},
		{
			keys:    []string{"127.0.0.1:2382", "mysql1"},
			adapter: UpstreamBoundWorkerKeyAdapter,
			want:    "/dm-master/v2/bound-worker/3132372e302e302e313a32333832/6d7973716c31",
		},
		{
			keys:    []string{"mysql1", "test"},
			adapter: UpstreamSubTaskKeyAdapter,
			want:    "/dm-master/upstream/subtask/6d7973716c31/74657374",
		},
		{
			keys:    []string{"test", "mysql_replica_01", "target_db", "target_table"},
			adapter: ShardDDLOptimismInfoKeyAdapter,
			want:    "/dm-master/shardddl-optimism/info/74657374/6d7973716c5f7265706c6963615f3031/7461726765745f6462/7461726765745f7461626c65",
		},
		{
			keys:    []string{"mysql/01"},
			adapter: StageRelayKeyAdapter,
			want:    "/dm-master/v2/stage/relay/6d7973716c2f3031",
		},
		{
			keys:    []string{"mysql1", "中文1🀄️"},
			adapter: UpstreamSubTaskKeyAdapter,
			want:    "/dm-master/upstream/subtask/6d7973716c31/e4b8ade6968731f09f8084efb88f",
		},
		{
			keys:    []string{"task-1"},
			adapter: OpenAPITaskTemplateKeyAdapter,
			want:    "/dm-master/openapi-task-template/7461736b2d31",
		},
	}

	for _, ca := range testCases {
		encKey := ca.adapter.Encode(ca.keys...)
		require.Equal(t, ca.want, encKey)
		decKey, err := ca.adapter.Decode(encKey)
		require.NoError(t, err)
		require.Equal(t, ca.keys, decKey)
	}
}

func TestEncodeAsPrefix(t *testing.T) {
	testCases := []struct {
		keys    []string
		adapter KeyAdapter
		want    string
	}{
		{
			keys:    []string{"mysql1"},
			adapter: UpstreamSubTaskKeyAdapter,
			want:    "/dm-master/upstream/subtask/6d7973716c31/",
		},
	}

	for _, ca := range testCases {
		encKey := ca.adapter.Encode(ca.keys...)
		require.Equal(t, ca.want, encKey)
		_, err := ca.adapter.Decode(encKey)
		require.Error(t, err)
	}
}

func TestIsErrNetClosing(t *testing.T) {
	server, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	err = server.Close()
	require.False(t, IsErrNetClosing(err))
	_, err = server.Accept()
	require.True(t, IsErrNetClosing(err))
}

func TestJoinUseSlash(t *testing.T) {
	// because we use "/" in Encode
	require.Equal(t, "a/b", path.Join("a", "b"))
}
