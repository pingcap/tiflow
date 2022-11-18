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
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/require"
)

type testMockCase struct {
	ev  *replication.BinlogEvent
	err error
}

func TestRead(t *testing.T) {
	t.Parallel()
	r := NewMockReader()

	// some interface methods do nothing
	require.Nil(t, r.StartSyncByPos(mysql.Position{}))
	require.Nil(t, r.StartSyncByGTID(nil))
	require.Nil(t, r.Status())
	require.Nil(t, r.Close())

	// replace with special error
	mockR := r.(*MockReader)
	errStartByPos := errors.New("special error for start by pos")
	errStartByGTID := errors.New("special error for start by GTID")
	errClose := errors.New("special error for close")
	mockR.ErrStartByPos = errStartByPos
	mockR.ErrStartByGTID = errStartByGTID
	mockR.ErrClose = errClose
	require.Equal(t, errStartByPos, r.StartSyncByPos(mysql.Position{}))
	require.Equal(t, errStartByGTID, r.StartSyncByGTID(nil))
	require.Equal(t, errClose, r.Close())

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
				require.Nil(t, mockR.PushError(ctx, cs.err))
			} else {
				require.Nil(t, mockR.PushEvent(ctx, cs.ev))
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
		require.Nil(t, ctx.Err())
		if len(obtained) == len(cases) {
			break
		}
	}

	require.Equal(t, cases, obtained)

	cancel() // cancel manually
	require.Equal(t, ctx.Err(), mockR.PushError(ctx, cases[0].err))
	require.Equal(t, ctx.Err(), mockR.PushEvent(ctx, cases[0].ev))
	ev, err := r.GetEvent(ctx)
	require.Nil(t, ev)
	require.Equal(t, ctx.Err(), err)
}
