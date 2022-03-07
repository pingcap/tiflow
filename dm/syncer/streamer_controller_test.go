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

package syncer

import (
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"

	"github.com/pingcap/tiflow/dm/relay"
)

func (s *testSyncerSuite) TestIsConnectionRefusedError(c *C) {
	isConnRefusedErr := isConnectionRefusedError(nil)
	c.Assert(isConnRefusedErr, Equals, false)

	isConnRefusedErr = isConnectionRefusedError(errors.New("timeout"))
	c.Assert(isConnRefusedErr, Equals, false)

	isConnRefusedErr = isConnectionRefusedError(errors.New("connect: connection refused"))
	c.Assert(isConnRefusedErr, Equals, true)
}

func (s *testSyncerSuite) TestCanErrorRetry(c *C) {
	relay2 := &relay.Relay{}
	controller := NewStreamerController(replication.BinlogSyncerConfig{}, true, nil, "", nil, relay2)

	mockErr := errors.New("test")

	// local binlog puller can always retry
	for i := 0; i < 5; i++ {
		c.Assert(controller.CanRetry(mockErr), IsTrue)
	}

	origCfg := minErrorRetryInterval
	minErrorRetryInterval = 100 * time.Millisecond
	defer func() {
		minErrorRetryInterval = origCfg
	}()

	// test with remote binlog
	controller = NewStreamerController(replication.BinlogSyncerConfig{}, true, nil, "", nil, nil)

	c.Assert(controller.CanRetry(mockErr), IsTrue)
	c.Assert(controller.CanRetry(mockErr), IsFalse)
	time.Sleep(100 * time.Millisecond)
	c.Assert(controller.CanRetry(mockErr), IsTrue)
}
