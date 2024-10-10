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

package relay

import (
	"context"
	"errors"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/check"
)

var _ = check.Suite(&testReaderRetrySuite{})

type testReaderRetrySuite struct{}

func (t *testReaderRetrySuite) TestRetry(c *check.C) {
	rr, err := NewReaderRetry(ReaderRetryConfig{
		BackoffRollback: 200 * time.Millisecond,
		BackoffMax:      1 * time.Second,
		BackoffMin:      1 * time.Millisecond,
		BackoffFactor:   2,
	})
	c.Assert(err, check.IsNil)
	c.Assert(rr, check.NotNil)

	retryableErr := gmysql.ErrBadConn
	unRetryableErr := errors.New("custom error")
	ctx := context.Background()

	// check some times
	for i := 0; i < 3; i++ {
		c.Assert(rr.Check(ctx, retryableErr), check.IsTrue)
	}
	c.Assert(rr.bf.Current(), check.Equals, 8*time.Millisecond)

	// check more times, until reach Max
	for i := 0; i < 10; i++ {
		c.Assert(rr.Check(ctx, retryableErr), check.IsTrue)
	}
	c.Assert(rr.bf.Current(), check.Equals, rr.cfg.BackoffMax)

	// sleep 1s
	time.Sleep(1 * time.Second)

	// check with rollback, rollback 5 times, forward 1 time
	c.Assert(rr.Check(ctx, retryableErr), check.IsTrue)
	c.Assert(rr.bf.Current(), check.Equals, 64*time.Millisecond)

	// check un-retryable error
	c.Assert(rr.Check(ctx, unRetryableErr), check.IsFalse)

	// check with context timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	c.Assert(rr.Check(ctx, retryableErr), check.IsFalse)
}
