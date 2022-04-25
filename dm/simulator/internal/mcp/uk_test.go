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
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/pingcap/tiflow/dm/pkg/log"
)

type testUniqueKeySuite struct {
	suite.Suite
}

func (s *testUniqueKeySuite) SetupSuite() {
	s.Require().Nil(log.InitLogger(&log.Config{}))
}

func (s *testUniqueKeySuite) TestUKClone() {
	origUKCol1Value := 111
	origUKCol2Value := "COL1"
	originalUK := &UniqueKey{
		rowID: -1,
		value: map[string]interface{}{
			"col1": origUKCol1Value,
			"col2": origUKCol2Value,
		},
	}
	newUKCol1Value := 222
	newUKCol2Value := "COL2"
	clonedUK := originalUK.Clone()
	clonedUK.value["col1"] = newUKCol1Value
	clonedUK.value["col2"] = newUKCol2Value

	s.T().Logf("original UK: %v; cloned UK: %v\n", originalUK, clonedUK)

	s.Equalf(origUKCol1Value, originalUK.value["col1"], "original.%s value incorrect", "col1")
	s.Equalf(origUKCol2Value, originalUK.value["col2"], "original.%s value incorrect", "col2")
	s.Equalf(newUKCol1Value, clonedUK.value["col1"], "cloned.%s value incorrect", "col1")
	s.Equalf(newUKCol2Value, clonedUK.value["col2"], "cloned.%s value incorrect", "col2")
}

func (s *testUniqueKeySuite) TestUKChangeBasic() {
	col1Value := 111
	col2Value := "aaa"
	theValueMap := map[string]interface{}{
		"col1": col1Value,
		"col2": col2Value,
	}
	theUK := NewUniqueKey(-1, theValueMap)
	s.Equalf(col1Value, theUK.value["col1"], "%s value incorrect", "col1")
	s.Equalf(col2Value, theUK.value["col2"], "%s value incorrect", "col2")

	theValueMap["col1"] = 222
	theValueMap["col2"] = "bbb"
	s.Equalf(col1Value, theUK.value["col1"], "%s value incorrect", "col1")
	s.Equalf(col2Value, theUK.value["col2"], "%s value incorrect", "col2")

	assignedValueMap := theUK.GetValue()
	s.Equalf(col1Value, assignedValueMap["col1"], "%s value incorrect", "col1")
	s.Equalf(col2Value, assignedValueMap["col2"], "%s value incorrect", "col2")

	newRowID := 999
	theUK.SetRowID(newRowID)
	s.Equal(newRowID, theUK.GetRowID(), "row ID value incorrect")

	newCol1Value := 333
	newCol2Value := "ccc"
	newValueMap := map[string]interface{}{
		"col1": newCol1Value,
		"col2": newCol2Value,
	}
	theUK.SetValue(newValueMap)
	s.Equalf(newCol1Value, theUK.value["col1"], "%s value incorrect", "col1")
	s.Equalf(newCol2Value, theUK.value["col2"], "%s value incorrect", "col2")
	s.Equalf(col1Value, assignedValueMap["col1"], "assigned map's %s value incorrect", "col1")
	s.Equalf(col2Value, assignedValueMap["col2"], "assigned map's %s value incorrect", "col2")

	newValueMap["col1"] = 444
	newValueMap["col2"] = "ddd"
	s.Equalf(newCol1Value, theUK.value["col1"], "%s value incorrect", "col1")
	s.Equalf(newCol2Value, theUK.value["col2"], "%s value incorrect", "col2")
}

func (s *testUniqueKeySuite) TestUKParallelChange() {
	theUK := NewUniqueKey(-1, nil)
	pendingCh := make(chan struct{})
	var wg sync.WaitGroup
	targetID := 100
	workerCnt := 10
	wg.Add(workerCnt)
	for i := 0; i < workerCnt; i++ {
		go func() {
			defer wg.Done()
			<-pendingCh
			for i := 1; i <= targetID; i++ {
				theUK.SetRowID(i)
				theUK.SetValue(map[string]interface{}{
					"id": i,
				})
			}
		}()
	}
	close(pendingCh)
	wg.Wait()
	s.Equal(targetID, theUK.GetRowID(), "row ID value incorrect")
	theValue := theUK.GetValue()
	s.Equal(targetID, theValue["id"], "ID column value incorrect")
	s.Equal(targetID, theUK.value["id"], "ID column value in UK incorrect")
}

func (s *testUniqueKeySuite) TestUKValueEqual() {
	col1Value := 111
	col2Value := "aaa"
	uk1 := &UniqueKey{
		rowID: -1,
		value: map[string]interface{}{
			"col1": col1Value,
			"col2": col2Value,
		},
	}
	uk2 := &UniqueKey{
		rowID: 100,
		value: map[string]interface{}{
			"col1": col1Value,
			"col2": col2Value,
		},
	}
	s.Equal(true, uk1.IsValueEqual(uk2), "uk1 should equal uk2 on value")
	s.Equal(true, uk2.IsValueEqual(uk1), "uk2 should equal uk1 on value")
	uk3 := &UniqueKey{
		rowID: 100,
		value: map[string]interface{}{
			"col1": col1Value,
			"col2": "bbb",
		},
	}
	s.Equal(false, uk1.IsValueEqual(uk3), "uk1 should not equal uk3 on value")
	s.Equal(false, uk3.IsValueEqual(uk1), "uk3 should not equal uk1 on value")
	uk4 := &UniqueKey{
		rowID: 100,
		value: map[string]interface{}{
			"col3": 321,
		},
	}
	s.Equal(false, uk1.IsValueEqual(uk4), "uk1 should not equal uk4 on value")
	s.Equal(false, uk4.IsValueEqual(uk1), "uk4 should not equal uk1 on value")
	uk5 := &UniqueKey{
		rowID: 100,
		value: map[string]interface{}{
			"col3": 321,
			"col1": col1Value,
			"col2": col2Value,
		},
	}
	s.Equal(false, uk1.IsValueEqual(uk5), "uk1 should not equal uk5 on value")
	s.Equal(false, uk5.IsValueEqual(uk1), "uk5 should not equal uk1 on value")
}

func TestUniqueKeySuite(t *testing.T) {
	suite.Run(t, &testUniqueKeySuite{})
}
