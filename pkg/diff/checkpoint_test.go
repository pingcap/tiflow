// Copyright 2024 PingCAP, Inc.
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

package diff

import (
	"context"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/check"
)

var _ = check.Suite(&testCheckpointSuite{})

type testCheckpointSuite struct{}

func (s *testUtilSuite) TestloadFromCheckPoint(c *check.C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, check.IsNil)

	rows := sqlmock.NewRows([]string{"state", "config_hash"}).AddRow("success", "123")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	useCheckpoint, err := loadFromCheckPoint(context.Background(), db, "test", "test", "123")
	c.Assert(err, check.IsNil)
	c.Assert(useCheckpoint, check.Equals, false)

	rows = sqlmock.NewRows([]string{"state", "config_hash"}).AddRow("success", "123")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	useCheckpoint, err = loadFromCheckPoint(context.Background(), db, "test", "test", "456")
	c.Assert(err, check.IsNil)
	c.Assert(useCheckpoint, check.Equals, false)

	rows = sqlmock.NewRows([]string{"state", "config_hash"}).AddRow("failed", "123")
	mock.ExpectQuery("SELECT").WillReturnRows(rows)
	useCheckpoint, err = loadFromCheckPoint(context.Background(), db, "test", "test", "123")
	c.Assert(err, check.IsNil)
	c.Assert(useCheckpoint, check.Equals, true)
}

func (s *testUtilSuite) TestInitChunks(c *check.C) {
	db, _, err := sqlmock.New()
	c.Assert(err, check.IsNil)

	chunks := []*ChunkRange{
		{
			ID:           1,
			Bounds:       []*Bound{{Column: "a", Lower: "1"}},
			State:        notCheckedState,
			columnOffset: map[string]int{"a": 0},
		}, {
			ID:           2,
			Bounds:       []*Bound{{Column: "a", Lower: "0", Upper: "1"}},
			State:        notCheckedState,
			columnOffset: map[string]int{"a": 0},
		},
	}

	// init chunks will insert chunk's information with update time, which use time.Now(), so can't know the value and can't fill the `WithArgs`
	// so just skip the `ExpectQuery` and check the error message
	// mock.ExpectQuery("INSERT INTO `sync_diff_inspector`.`chunk` VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?), (?, ?, ?, ?, ?, ?, ?, ?, ?)").WithArgs(......)
	err = initChunks(context.Background(), db, "target", "diff_test", "test", chunks)
	c.Assert(err, check.ErrorMatches, ".*INSERT INTO `sync_diff_inspector`.`chunk` VALUES\\(\\?, \\?, \\?, \\?, \\?, \\?, \\?, \\?, \\?\\), \\(\\?, \\?, \\?, \\?, \\?, \\?, \\?, \\?, \\?\\).*")
}
