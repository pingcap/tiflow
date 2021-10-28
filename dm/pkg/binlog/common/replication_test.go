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

package common

import (
	"github.com/go-mysql-org/go-mysql/replication"
	. "github.com/pingcap/check"
)

func (t *testCommonSuite) TestSetDefaultReplicationCfg(c *C) {
	syncCfg := replication.BinlogSyncerConfig{}

	retryCnt := 5
	SetDefaultReplicationCfg(&syncCfg, retryCnt)
	c.Assert(syncCfg.MaxReconnectAttempts, Equals, retryCnt)
	c.Assert(syncCfg.DisableRetrySync, IsFalse)

	retryCnt = 1
	SetDefaultReplicationCfg(&syncCfg, retryCnt)
	c.Assert(syncCfg.MaxReconnectAttempts, Equals, retryCnt)
	c.Assert(syncCfg.DisableRetrySync, IsTrue)
}
