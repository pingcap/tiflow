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
package etcd

import (
	"math"
	"math/rand"

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"go.etcd.io/etcd/clientv3"
)

type utilSuit struct{}

var _ = check.Suite(&utilSuit{})

func (s utilSuit) TestGetRevisionFromWatchOpts(c *check.C) {
	defer testleak.AfterTest(c)()
	for i := 0; i < 100; i++ {
		rev := rand.Int63n(math.MaxInt64)
		opt := clientv3.WithRev(rev)
		c.Assert(getRevisionFromWatchOpts(opt), check.Equals, rev)
	}
}
