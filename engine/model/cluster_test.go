// Copyright 2022 PingCAP, Inc.
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
	"encoding/hex"
	"testing"

	"github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/pkg/label"
	"github.com/stretchr/testify/require"
)

func TestNodeInfoEtcdKey(t *testing.T) {
	t.Parallel()

	nodeInfo := &NodeInfo{
		Type: NodeTypeExecutor,
		ID:   "executor-id-1",
	}
	encoded := hex.EncodeToString([]byte("executor-id-1"))
	require.Equal(t, "/data-flow/node/info/"+encoded, nodeInfo.EtcdKey())

	nodeInfo = &NodeInfo{
		Type: NodeTypeServerMaster,
		ID:   "server-master-id-1",
	}
	encoded = hex.EncodeToString([]byte("server-master-id-1"))
	require.Equal(t, "/data-flow/node/info/"+encoded, nodeInfo.EtcdKey())
}

func TestNodeInfoFromRegisterExecutorRequest(t *testing.T) {
	t.Parallel()

	nodeInfo := NewNodeInfoForExecutor("executor-1")
	require.Equal(t, NodeTypeExecutor, nodeInfo.Type)

	req := &enginepb.RegisterExecutorRequest{
		Address:    "123.123.123.123:1234",
		Version:    "v6.1.2",
		Capability: 15,
		Labels: map[string]string{
			"label1": "value1",
			"label2": "value2",
		},
	}
	err := nodeInfo.FromRegisterExecutorRequest(req)
	require.NoError(t, err)

	val, ok := nodeInfo.Labels.Get("label1")
	require.True(t, ok)
	require.Equal(t, label.Value("value1"), val)

	val, ok = nodeInfo.Labels.Get("label2")
	require.True(t, ok)
	require.Equal(t, label.Value("value2"), val)
}

func TestNodeInfoFromRegisterExecutorRequestError(t *testing.T) {
	t.Parallel()

	nodeInfo := NewNodeInfoForExecutor("executor-1")
	require.Equal(t, NodeTypeExecutor, nodeInfo.Type)

	req := &enginepb.RegisterExecutorRequest{
		Address:    "123.123.123.123:1234",
		Version:    "v6.1.2",
		Capability: 15,
		Labels: map[string]string{
			"~": "value1", // invalid
		},
	}
	err := nodeInfo.FromRegisterExecutorRequest(req)
	require.ErrorContains(t, err, "FromRegisterExecutorRequest: ")
}

func TestNodeInfoToJSON(t *testing.T) {
	t.Parallel()

	nodeInfo := NewNodeInfoForExecutor("executor-1")
	require.Equal(t, NodeTypeExecutor, nodeInfo.Type)

	req := &enginepb.RegisterExecutorRequest{
		Address:    "123.123.123.123:1234",
		Version:    "v6.1.2",
		Capability: 15,
		Labels: map[string]string{
			"label1": "value1",
			"label2": "value2",
		},
	}
	err := nodeInfo.FromRegisterExecutorRequest(req)
	require.NoError(t, err)

	expected := `{"type":2,"id":"executor-1","addr":"123.123.123.123:1234",` +
		`"cap":15,"labels":{"label1":"value1","label2":"value2"}}`
	actual, err := nodeInfo.ToJSON()
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}
