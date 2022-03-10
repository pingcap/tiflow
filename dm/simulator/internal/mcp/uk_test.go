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

package mcp

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/tiflow/dm/pkg/log"
)

type testUniqueKeySuite struct {
	suite.Suite
}

func (s *testUniqueKeySuite) SetupSuite() {
	assert.Nil(s.T(), log.InitLogger(&log.Config{}))
}

func (s *testUniqueKeySuite) TestUKClone() {
	origUKCol1Value := 111
	origUKCol2Value := "COL1"
	originalUK := &UniqueKey{
		RowID: -1,
		Value: map[string]interface{}{
			"col1": origUKCol1Value,
			"col2": origUKCol2Value,
		},
	}
	newUKCol1Value := 222
	newUKCol2Value := "COL2"
	clonedUK := originalUK.Clone()
	clonedUK.Value["col1"] = newUKCol1Value
	clonedUK.Value["col2"] = newUKCol2Value

	s.T().Logf("original UK: %v; cloned UK: %v\n", originalUK, clonedUK)

	assert.Equal(s.T(), originalUK.Value["col1"], origUKCol1Value, fmt.Sprintf("original.col1 should be %d", origUKCol1Value))
	assert.Equal(s.T(), originalUK.Value["col2"], origUKCol2Value, fmt.Sprintf("original.col2 should be %s", origUKCol2Value))
	assert.Equal(s.T(), clonedUK.Value["col1"], newUKCol1Value, fmt.Sprintf("cloned.col1 should be %d", newUKCol1Value))
	assert.Equal(s.T(), clonedUK.Value["col2"], newUKCol2Value, fmt.Sprintf("cloned.col2 should be %s", newUKCol2Value))
}

func (s *testUniqueKeySuite) TestUKValueEqual() {
	col1Value := 111
	col2Value := "aaa"
	uk1 := &UniqueKey{
		RowID: -1,
		Value: map[string]interface{}{
			"col1": col1Value,
			"col2": col2Value,
		},
	}
	uk2 := &UniqueKey{
		RowID: 100,
		Value: map[string]interface{}{
			"col1": col1Value,
			"col2": col2Value,
		},
	}
	assert.Equal(s.T(), uk1.IsValueEqual(uk2), true, "uk1 should equal uk2 on value")
	assert.Equal(s.T(), uk2.IsValueEqual(uk1), true, "uk2 should equal uk1 on value")
	uk3 := &UniqueKey{
		RowID: 100,
		Value: map[string]interface{}{
			"col1": col1Value,
			"col2": "bbb",
		},
	}
	assert.Equal(s.T(), uk1.IsValueEqual(uk3), false, "uk1 should not equal uk3 on value")
	assert.Equal(s.T(), uk3.IsValueEqual(uk1), false, "uk3 should not equal uk1 on value")
	uk4 := &UniqueKey{
		RowID: 100,
		Value: map[string]interface{}{
			"col3": 321,
		},
	}
	assert.Equal(s.T(), uk1.IsValueEqual(uk4), false, "uk1 should not equal uk4 on value")
	assert.Equal(s.T(), uk4.IsValueEqual(uk1), false, "uk4 should not equal uk1 on value")
	uk5 := &UniqueKey{
		RowID: 100,
		Value: map[string]interface{}{
			"col3": 321,
			"col1": col1Value,
			"col2": col2Value,
		},
	}
	assert.Equal(s.T(), uk1.IsValueEqual(uk5), false, "uk1 should not equal uk5 on value")
	assert.Equal(s.T(), uk5.IsValueEqual(uk1), false, "uk5 should not equal uk1 on value")
}

func TestUniqueKeySUite(t *testing.T) {
	suite.Run(t, &testUniqueKeySuite{})
}
