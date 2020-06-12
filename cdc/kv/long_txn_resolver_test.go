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

package kv

import (
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/cdcpb"
)

type LongTxnResolverSuite struct{}

var _ = check.Suite(&LongTxnResolverSuite{})

func (s *LongTxnResolverSuite) TxnStatusCache(c *check.C) {
	cache := NewTxnStatusCache(time.Millisecond * 200)

	makeTxnStatus := func(startTs, minCommitTs, commitTs uint64) *cdcpb.TxnStatus {
		return &cdcpb.TxnStatus{
			StartTs:     startTs,
			MinCommitTs: minCommitTs,
			CommitTs:    commitTs,
		}
	}

	makeTxnInfo := func(startTs uint64) *cdcpb.TxnInfo {
		return &cdcpb.TxnInfo{
			StartTs: startTs,
		}
	}

	cache.Update([]*cdcpb.TxnStatus{
		makeTxnStatus(1, 2, 0),
		makeTxnStatus(3, 4, 0),
	})

	res, rem := cache.Get([]*cdcpb.TxnInfo{
		makeTxnInfo(1),
		makeTxnInfo(3),
	})
	c.Assert(res, check.DeepEquals, []*cdcpb.TxnStatus{
		makeTxnStatus(1, 2, 0),
		makeTxnStatus(3, 4, 0),
	})
	c.Assert(len(rem), check.Equals, 0)

	res, rem = cache.Get([]*cdcpb.TxnInfo{
		makeTxnInfo(1),
		makeTxnInfo(2),
	})

	c.Assert(res, check.DeepEquals, []*cdcpb.TxnStatus{
		makeTxnStatus(1, 2, 0),
	})
	c.Assert(rem, check.DeepEquals, []*cdcpb.TxnInfo{
		makeTxnInfo(2),
	})

	// Update cache
	cache.Update([]*cdcpb.TxnStatus{
		makeTxnStatus(3, 10, 0),
	})
	res, rem = cache.Get([]*cdcpb.TxnInfo{
		makeTxnInfo(1),
		makeTxnInfo(3),
	})
	c.Assert(res, check.DeepEquals, []*cdcpb.TxnStatus{
		makeTxnStatus(1, 2, 0),
		makeTxnStatus(3, 10, 0),
	})
	c.Assert(len(rem), check.Equals, 0)

	cache.Update([]*cdcpb.TxnStatus{
		makeTxnStatus(3, 0, 10),
	})
	res, rem = cache.Get([]*cdcpb.TxnInfo{
		makeTxnInfo(3),
	})
	c.Assert(res, check.DeepEquals, []*cdcpb.TxnStatus{
		makeTxnStatus(3, 0, 10),
	})
	c.Assert(len(rem), check.Equals, 0)

	// Expiring
	time.Sleep(time.Millisecond * 300)
	res, rem = cache.Get([]*cdcpb.TxnInfo{
		makeTxnInfo(1),
		makeTxnInfo(3),
	})
	c.Assert(len(res), check.Equals, 0)
	c.Assert(rem, check.DeepEquals, []*cdcpb.TxnInfo{
		makeTxnInfo(1),
		makeTxnInfo(3),
	})
}
