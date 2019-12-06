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

package sink

import (
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
)

func TestSuite(t *testing.T) { check.TestingT(t) }

type UtilSuite struct{}

var _ = check.Suite(&UtilSuite{})

func (cs *UtilSuite) SetUpTest(c *check.C) {
}

func (cs *UtilSuite) TestGetTableInfoTableNotExist(c *check.C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, check.IsNil)

	defer db.Close()

	// return empty rows
	columnRows := sqlmock.NewRows([]string{"Field", "Extra"})
	mock.ExpectQuery(regexp.QuoteMeta(colsSQL)).WithArgs("test", "test1").WillReturnRows(columnRows)

	_, err = getTableInfo(db, "test", "test1")
	c.Assert(errors.Cause(err), check.Equals, errTableNotExist)

}

func (cs *UtilSuite) TestGetTableInfo(c *check.C) {
	db, mock, err := sqlmock.New()

	c.Assert(err, check.IsNil)
	defer db.Close()

	// (id, a1, a2, a3, a4)
	// primary key: id
	// unique key: (a1) (a2,a3)
	columnRows := sqlmock.NewRows([]string{"Field", "Extra"}).
		AddRow("id", "").
		AddRow("a1", "").
		AddRow("a2", "").
		AddRow("a3", "VIRTUAL GENERATED").
		AddRow("a4", "")
	mock.ExpectQuery(regexp.QuoteMeta(colsSQL)).WithArgs("test", "test1").WillReturnRows(columnRows)

	indexRows := sqlmock.NewRows([]string{"non_unique", "index_name", "seq_in_index", "column_name"}).
		AddRow(0, "dex1", 1, "a1").
		AddRow(0, "PRIMARY", 1, "id").
		AddRow(0, "dex2", 1, "a2").
		AddRow(1, "dex3", 1, "a4").
		AddRow(0, "dex2", 2, "a3")

	mock.ExpectQuery(regexp.QuoteMeta(uniqKeysSQL)).WithArgs("test", "test1").WillReturnRows(indexRows)

	info, err := getTableInfo(db, "test", "test1")
	c.Assert(err, check.IsNil)
	c.Assert(info, check.NotNil)

	c.Assert(info, check.DeepEquals, &tableInfo{
		columns:    []string{"id", "a1", "a2", "a4"}, // generated column a3 is ignored
		primaryKey: &indexInfo{"PRIMARY", []string{"id"}},
		uniqueKeys: []indexInfo{{"PRIMARY", []string{"id"}},
			{"dex1", []string{"a1"}},
			{"dex2", []string{"a2", "a3"}},
		}})
}
