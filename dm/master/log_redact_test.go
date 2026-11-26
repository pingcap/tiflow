// Copyright 2026 PingCAP, Inc.
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

package master

import (
	"testing"

	"github.com/pingcap/tiflow/dm/pb"
	"github.com/stretchr/testify/require"
)

func TestRedactConfigYAML(t *testing.T) {
	in := `name: task
target-db:
  host: 127.0.0.1
  password: top-secret
  security:
    ssl-ca-bytes: ca-material
    ssl-key-base64: key-material
loaders:
  loader-1:
    dir: s3://bucket/path?access-key=ak-secret&secret-access-key=sk-secret
`
	out := redactConfigYAML(in)
	require.NotContains(t, out, "top-secret")
	require.NotContains(t, out, "ca-material")
	require.NotContains(t, out, "key-material")
	require.NotContains(t, out, "ak-secret")
	require.NotContains(t, out, "sk-secret")
	require.Contains(t, out, "127.0.0.1")
	require.Contains(t, out, "task")

	require.Equal(t, "(invalid config; redacted)", redactConfigYAML("not: [valid"))
	require.Equal(t, "  ", redactConfigYAML("  "))
}

func TestRedactedPayload(t *testing.T) {
	req := &pb.StartTaskRequest{
		Task:    "name: task\ntarget-db:\n  password: secret\n",
		Sources: []string{"mysql-01"},
	}
	out := redactedPayload(req)
	require.NotContains(t, out, "secret")
	require.Contains(t, out, "mysql-01")
	require.Contains(t, out, "task")

	src := &pb.OperateSourceRequest{
		Op:       pb.SourceOp_StartSource,
		Config:   []string{"source-id: mysql-01\nfrom:\n  password: secret\n"},
		SourceID: []string{"mysql-01"},
	}
	out = redactedPayload(src)
	require.NotContains(t, out, "secret")
	require.Contains(t, out, "mysql-01")

	require.Equal(t, "(plaintext hidden)", redactedPayload(&pb.EncryptRequest{Plaintext: "secret"}))
	require.Equal(t, "(*pb.GetCfgRequest)", redactedPayload(&pb.GetCfgRequest{}))
}
