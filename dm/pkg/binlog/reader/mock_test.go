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

package reader

import (
	"context"
	"errors"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	. "github.com/pingcap/check"
)

var _ = Suite(&testMockReaderSuite{})

type testMockReaderSuite struct{}

type testMockCase struct {
	ev  *replication.BinlogEvent
	err error
}

func (t *testMockReaderSuite) TestRead(c *C) {
	r := NewMockReader()

	// some interface methods do nothing
	c.Assert(r.StartSyncByPos(mysql.Position{}), IsNil)
	c.Assert(r.StartSyncByGTID(nil), IsNil)
	c.Assert(r.Status(), IsNil)
	c.Assert(r.Close(), IsNil)

	// replace with special error
	mockR := r.(*MockReader)
	errStartByPos := errors.New("special error for start by pos")
	errStartByGTID := errors.New("special error for start by GTID")
	errClose := errors.New("special error for close")
	mockR.ErrStartByPos = errStartByPos
	mockR.ErrStartByGTID = errStartByGTID
	mockR.ErrClose = errClose
	c.Assert(r.StartSyncByPos(mysql.Position{}), Equals, errStartByPos)
	c.Assert(r.StartSyncByGTID(nil), Equals, errStartByGTID)
	c.Assert(r.Close(), Equals, errClose)

	cases := []testMockCase{
		{
			ev: &replication.BinlogEvent{
				RawData: []byte{1},
			},
			err: nil,
		},
		{
			ev: &replication.BinlogEvent{
				RawData: []byte{2},
			},
			err: nil,
		},
		{
			ev:  nil,
			err: errors.New("1"),
		},
		{
			ev: &replication.BinlogEvent{
				RawData: []byte{3},
			},
			err: nil,
		},
		{
			ev:  nil,
			err: errors.New("2"),
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	go func() {
		for _, cs := range cases {
			if cs.err != nil {
				c.Assert(mockR.PushError(ctx, cs.err), IsNil)
			} else {
				c.Assert(mockR.PushEvent(ctx, cs.ev), IsNil)
			}
		}
	}()

	obtained := make([]testMockCase, 0, len(cases))
	for {
		ev, err := r.GetEvent(ctx)
		if err != nil {
			obtained = append(obtained, testMockCase{ev: nil, err: err})
		} else {
			obtained = append(obtained, testMockCase{ev: ev, err: nil})
		}
		c.Assert(ctx.Err(), IsNil)
		if len(obtained) == len(cases) {
			break
		}
	}

	c.Assert(obtained, DeepEquals, cases)

	cancel() // cancel manually
	c.Assert(mockR.PushError(ctx, cases[0].err), Equals, ctx.Err())
	c.Assert(mockR.PushEvent(ctx, cases[0].ev), Equals, ctx.Err())
	ev, err := r.GetEvent(ctx)
	c.Assert(ev, IsNil)
	c.Assert(err, Equals, ctx.Err())
}
