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

package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/test"
	"github.com/stretchr/testify/require"
)

func init() {
	// initialized the logger to make genEmbedEtcdConfig working.
	err := log.InitLogger(&log.Config{})
	if err != nil {
		panic(err)
	}
}

func TestEtcdSession(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	name := "test-etcd-session"
	_, _, client, cleanFn := test.PrepareEtcd(t, name)
	defer cleanFn()

	cfg := &EtcdSessionConfig{
		Member:       "member",
		Key:          "session-key",
		Value:        "session-value",
		KeepaliveTTL: 5 * time.Second,
	}
	session, err := NewEtcdSession(ctx, client, cfg)
	require.Nil(t, err)
	_, _, err = session.Campaign(ctx, time.Second)
	require.Nil(t, err)

	// session lease expired, campaign will fail
	_, err = client.Revoke(ctx, session.session.Lease())
	require.Nil(t, err)
	_, _, err = session.Campaign(ctx, time.Second)
	require.Regexp(t, "etcdserver: requested lease not found$", err)

	// check whether need to reset session, reset session and campaign again
	needReset := session.CheckNeedReset(err)
	require.True(t, needReset)
	err = session.Reset(ctx)
	require.Nil(t, err)
	_, _, err = session.Campaign(ctx, time.Second)
	require.Nil(t, err)
}
