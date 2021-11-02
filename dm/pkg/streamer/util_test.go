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

package streamer

import (
	"io"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
)

var _ = Suite(&testUtilSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testUtilSuite struct{}

func (t *testUtilSuite) TestGetNextUUID(c *C) {
	UUIDs := []string{
		"b60868af-5a6f-11e9-9ea3-0242ac160006.000001",
		"7acfedb5-3008-4fa2-9776-6bac42b025fe.000002",
		"92ffd03b-813e-4391-b16a-177524e8d531.000003",
		"338513ce-b24e-4ff8-9ded-9ac5aa8f4d74.000004",
	}
	cases := []struct {
		currUUID       string
		UUIDs          []string
		nextUUID       string
		nextUUIDSuffix string
		errMsgReg      string
	}{
		{
			// empty current and UUID list
		},
		{
			// non-empty current UUID, but empty UUID list
			currUUID: "b60868af-5a6f-11e9-9ea3-0242ac160006.000001",
		},
		{
			// empty current UUID, but non-empty UUID list
			UUIDs: UUIDs,
		},
		{
			// current UUID in UUID list, has next UUID
			currUUID:       UUIDs[0],
			UUIDs:          UUIDs,
			nextUUID:       UUIDs[1],
			nextUUIDSuffix: UUIDs[1][len(UUIDs[1])-6:],
		},
		{
			// current UUID in UUID list, but has no next UUID
			currUUID: UUIDs[len(UUIDs)-1],
			UUIDs:    UUIDs,
		},
		{
			// current UUID not in UUID list
			currUUID: "40ed16c1-f6f7-4012-aa9b-d360261d2b22.666666",
			UUIDs:    UUIDs,
		},
		{
			// invalid next UUID in UUID list
			currUUID:  UUIDs[len(UUIDs)-1],
			UUIDs:     append(UUIDs, "invalid-uuid"),
			errMsgReg: ".*invalid-uuid.*",
		},
	}

	for _, cs := range cases {
		nu, nus, err := getNextUUID(cs.currUUID, cs.UUIDs)
		if len(cs.errMsgReg) > 0 {
			c.Assert(err, ErrorMatches, cs.errMsgReg)
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(nu, Equals, cs.nextUUID)
		c.Assert(nus, Equals, cs.nextUUIDSuffix)
	}
}

func (t *testUtilSuite) TestIsIgnorableParseError(c *C) {
	cases := []struct {
		err       error
		ignorable bool
	}{
		{
			err:       nil,
			ignorable: false,
		},
		{
			err:       io.EOF,
			ignorable: true,
		},
		{
			err:       errors.Annotate(io.EOF, "annotated end of file"),
			ignorable: true,
		},
		{
			err:       errors.New("get event header err EOF xxxx"),
			ignorable: true,
		},
		{
			err:       errors.New("some other error"),
			ignorable: false,
		},
	}

	for _, cs := range cases {
		c.Assert(isIgnorableParseError(cs.err), Equals, cs.ignorable)
	}
}
