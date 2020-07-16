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
	c.Assert(err, check.ErrorMatches, "empty PD address")
	c.Assert(svr, check.IsNil)

	svr, err = NewServer(PDEndpoints("http://pd"))
	c.Assert(err, check.ErrorMatches, "empty address")
	c.Assert(svr, check.IsNil)

	svr, err = NewServer(PDEndpoints("http://pd"), Address("cdc:1234"))
	c.Assert(err, check.ErrorMatches, "empty GC TTL is not allowed")
	c.Assert(svr, check.IsNil)

	svr, err = NewServer(PDEndpoints("http://pd"), Address("cdc:1234"), GCTTL(DefaultCDCGCSafePointTTL))
	c.Assert(err, check.IsNil)
	c.Assert(svr, check.NotNil)
	c.Assert(svr.opts.advertiseAddr, check.Equals, "cdc:1234")

	svr, err = NewServer(PDEndpoints("http://pd"), Address("cdc:1234"), GCTTL(DefaultCDCGCSafePointTTL),
		AdvertiseAddress("advertise:1234"))
	c.Assert(err, check.IsNil)
	c.Assert(svr, check.NotNil)
	c.Assert(svr.opts.addr, check.Equals, "cdc:1234")
	c.Assert(svr.opts.advertiseAddr, check.Equals, "advertise:1234")

	svr, err = NewServer(PDEndpoints("http://pd"), Address("0.0.0.0:1234"), GCTTL(DefaultCDCGCSafePointTTL),
		AdvertiseAddress("advertise:1234"))
	c.Assert(err, check.IsNil)
	c.Assert(svr, check.NotNil)
	c.Assert(svr.opts.addr, check.Equals, "0.0.0.0:1234")
	c.Assert(svr.opts.advertiseAddr, check.Equals, "advertise:1234")

	svr, err = NewServer(PDEndpoints("http://pd"), Address("0.0.0.0:1234"), GCTTL(DefaultCDCGCSafePointTTL))
	c.Assert(err, check.ErrorMatches, ".*must be specified.*")
	c.Assert(svr, check.IsNil)

	svr, err = NewServer(PDEndpoints("http://pd"), Address("cdc:1234"), GCTTL(DefaultCDCGCSafePointTTL),
		AdvertiseAddress("0.0.0.0:1234"))
	c.Assert(err, check.ErrorMatches, ".*must be specified.*")
	c.Assert(svr, check.IsNil)

	svr, err = NewServer(PDEndpoints("http://pd"), Address("cdc:1234"), GCTTL(DefaultCDCGCSafePointTTL),
		AdvertiseAddress("advertise"))
	c.Assert(err, check.ErrorMatches, ".*does not contain a port")
	c.Assert(svr, check.IsNil)
}
