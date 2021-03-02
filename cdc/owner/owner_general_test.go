// Copyright 2021 PingCAP, Inc.
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

package owner

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"github.com/pingcap/tidb/store/mockstore"
	"go.uber.org/zap/zapcore"
)

func TestSuite(t *testing.T) { check.TestingT(t) }

type ownerSuite struct {
}

var _ = check.Suite(&ownerSuite{})

func (s *ownerSuite) TestOwnerInitialization(c *check.C) {
	defer testleak.AfterTest(c)()

	info := &model.ChangeFeedInfo{
		SinkURI:    "blackhole:///",
		Opts:       make(map[string]string),
		CreateTime: time.Now(),
		StartTs:    2000,
		TargetTs:   0,
		Config:     config.GetDefaultReplicaConfig(),
		Engine:     model.SortUnified,
		SortDir:    ".",
		State:      model.StateNormal,
	}

	log.SetLevel(zapcore.DebugLevel)
	harness := newOwnerTestHarness(c.MkDir())
	owner := harness.CreateOwner(1000)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	go func() {
		etcdCli := harness.newClient()
		cdcClient := kv.NewCDCEtcdClient(ctx, etcdCli.Unwrap())
		err := cdcClient.CreateChangefeedInfo(ctx, info, "test-1")
		c.Assert(err, check.IsNil)
	}()

	store, err := mockstore.NewMockStore()
	c.Assert(err, check.IsNil)

	ctx = util.PutKVStorageInCtx(ctx, store)
	err = owner.Run(ctx)
	c.Assert(err, check.ErrorMatches, ".*deadline exceeded.*")
}
