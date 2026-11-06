// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeValuesFromBucketsCompatibility(t *testing.T) {
	testCases := []struct {
		value string
		types []byte
	}{
		{value: "private-value", types: []byte{mysql.TypeVarchar}},
		{value: "(7, private-value)", types: []byte{mysql.TypeLong, mysql.TypeVarchar}},
		{value: "2021-03-05 21:31:03", types: []byte{mysql.TypeDatetime}},
		{value: "2021-03-05 21:31:03", types: []byte{mysql.TypeTimestamp}},
		{value: "2021-03-05", types: []byte{mysql.TypeDate}},
		{value: "1847956477067657216", types: []byte{mysql.TypeDatetime}},
		{value: "1847955927311843328", types: []byte{mysql.TypeTimestamp}},
		{value: "1847955789872889856", types: []byte{mysql.TypeDate}},
		{value: "0", types: []byte{mysql.TypeTimestamp}},
		{value: "(7, private-value)", types: []byte{mysql.TypeLong}},
	}
	for _, tc := range testCases {
		t.Run(tc.value, func(t *testing.T) {
			cols := make([]*model.ColumnInfo, len(tc.types))
			for i, tp := range tc.types {
				cols[i] = &model.ColumnInfo{FieldType: *types.NewFieldType(tp)}
			}
			expectedValues, expectedErr := dbutil.AnalyzeValuesFromBuckets(tc.value, cols)
			actualValues, actualErr := AnalyzeValuesFromBuckets(tc.value, cols)
			require.Equal(t, expectedValues, actualValues)
			if expectedErr != nil {
				require.EqualError(t, actualErr, expectedErr.Error())
			} else {
				require.NoError(t, actualErr)
			}
		})
	}
}
