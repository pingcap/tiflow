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

package cdc

import (
	"github.com/pingcap/check"
)

type serverOptionSuite struct{}

var _ = check.Suite(&serverOptionSuite{})

func (s *serverOptionSuite) TestNewServer(c *check.C) {
	svr, err := NewServer()
	c.Assert(svr, check.IsNil)
	c.Assert(err, check.ErrorMatches, "empty PD address")

	svr, err = NewServer(PDEndpoints("pd"))
	c.Assert(svr, check.IsNil)
	c.Assert(err, check.ErrorMatches, "empty address")

	svr, err = NewServer(PDEndpoints("pd"), Address("cdc"))
	c.Assert(svr, check.IsNil)
	c.Assert(err, check.ErrorMatches, "empty GC TTL is not allowed")

	svr, err = NewServer(PDEndpoints("pd"), Address("cdc"), GCTTL(DefaultCDCGCSafePointTTL))
	c.Assert(svr, check.NotNil)
	c.Assert(err, check.IsNil)
	c.Assert(svr.opts.advertiseAddr, check.Equals, "cdc")

	svr, err = NewServer(PDEndpoints("pd"), Address("cdc"), GCTTL(DefaultCDCGCSafePointTTL),
		AdvertiseAddress("advertise"))
	c.Assert(svr, check.NotNil)
	c.Assert(err, check.IsNil)
	c.Assert(svr.opts.addr, check.Equals, "cdc")
	c.Assert(svr.opts.advertiseAddr, check.Equals, "advertise")
}
