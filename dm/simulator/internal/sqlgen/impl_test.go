// Copyright 2022 PingCAP, Inc.
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

package sqlgen

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/simulator/internal/config"
	"github.com/pingcap/tiflow/dm/simulator/internal/mcp"
)

type testSQLGenImplSuite struct {
	suite.Suite
	tableConfig *config.TableConfig
}

func (s *testSQLGenImplSuite) SetupSuite() {
	assert.Nil(s.T(), log.InitLogger(&log.Config{}))
	s.tableConfig = &config.TableConfig{
		DatabaseName: "games",
		TableName:    "members",
		Columns: []*config.ColumnDefinition{
			&config.ColumnDefinition{
				ColumnName: "id",
				DataType:   "int",
				DataLen:    11,
			},
			&config.ColumnDefinition{
				ColumnName: "name",
				DataType:   "varchar",
				DataLen:    255,
			},
			&config.ColumnDefinition{
				ColumnName: "age",
				DataType:   "int",
				DataLen:    11,
			},
			&config.ColumnDefinition{
				ColumnName: "team_id",
				DataType:   "int",
				DataLen:    11,
			},
		},
		UniqueKeyColumnNames: []string{"id"},
	}
}

func (s *testSQLGenImplSuite) TestDMLBasic() {
	var (
		err error
		sql string
		uk  *mcp.UniqueKey
	)
	g := NewSQLGeneratorImpl(s.tableConfig)

	sql, _, err = g.GenLoadUniqueKeySQL()
	assert.Nil(s.T(), err)
	s.T().Logf("Generated SELECT SQL: %s\n", sql)

	sql, err = g.GenTruncateTable()
	assert.Nil(s.T(), err)
	s.T().Logf("Generated Truncate Table SQL: %s\n", sql)

	theMCP := mcp.NewModificationCandidatePool()
	for i := 0; i < 4096; i++ {
		assert.Nil(s.T(),
			theMCP.AddUK(&mcp.UniqueKey{
				RowID: i,
				Value: map[string]interface{}{
					"id": i,
				},
			}),
		)
	}
	for i := 0; i < 10; i++ {
		uk = theMCP.NextUK()
		sql, err = g.GenUpdateRow(uk)
		assert.Nil(s.T(), err)
		s.T().Logf("Generated SQL: %s\n", sql)
		sql, uk, err = g.GenInsertRow()
		assert.Nil(s.T(), err)
		s.T().Logf("Generated SQL: %s\n; Unique key: %v\n", sql, uk)
		uk = theMCP.NextUK()
		sql, err = g.GenDeleteRow(uk)
		assert.Nil(s.T(), err)
		s.T().Logf("Generated SQL: %s\n; Unique key: %v\n", sql, uk)
	}
}

func (s *testSQLGenImplSuite) TestDMLAbnormalUK() {
	var (
		sql string
		err error
		uk  *mcp.UniqueKey
	)
	g := NewSQLGeneratorImpl(s.tableConfig)
	uk = &mcp.UniqueKey{
		RowID: -1,
		Value: map[string]interface{}{
			"abcdefg": 123,
		},
	}
	_, err = g.GenUpdateRow(uk)
	assert.NotNil(s.T(), err)
	_, err = g.GenDeleteRow(uk)
	assert.NotNil(s.T(), err)

	uk = &mcp.UniqueKey{
		RowID: -1,
		Value: map[string]interface{}{
			"id":      123,
			"abcdefg": 321,
		},
	}
	sql, err = g.GenUpdateRow(uk)
	assert.Nil(s.T(), err)
	s.T().Logf("Generated SQL: %s\n", sql)
	sql, err = g.GenDeleteRow(uk)
	assert.Nil(s.T(), err)
	s.T().Logf("Generated SQL: %s\n", sql)
}

func TestSQLGenImplSuite(t *testing.T) {
	suite.Run(t, &testSQLGenImplSuite{})
}
