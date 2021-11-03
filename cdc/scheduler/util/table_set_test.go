// Copyright 2021 PingCAP, Inc.
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

package util

import (
	"testing"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestTableSetBasics(t *testing.T) {
	ts := NewTableSet()
	ok := ts.AddTableRecord(&TableRecord{
		TableID:   1,
		CaptureID: "capture-1",
		Status:    0,
	})
	require.True(t, ok)

	ok = ts.AddTableRecord(&TableRecord{
		TableID:   1,
		CaptureID: "capture-2",
		Status:    0,
	})
	// Adding a duplicate table record should fail
	require.False(t, ok)

	record, ok := ts.GetTableRecord(1)
	require.True(t, ok)
	require.Equal(t, &TableRecord{
		TableID:   1,
		CaptureID: "capture-1",
		Status:    0,
	}, record)

	ok = ts.RemoveTableRecord(1)
	require.True(t, ok)

	ok = ts.RemoveTableRecord(2)
	require.False(t, ok)
}

func TestTableSetCaptures(t *testing.T) {
	ts := NewTableSet()
	ok := ts.AddTableRecord(&TableRecord{
		TableID:   1,
		CaptureID: "capture-1",
		Status:    0,
	})
	require.True(t, ok)

	ok = ts.AddTableRecord(&TableRecord{
		TableID:   2,
		CaptureID: "capture-1",
		Status:    0,
	})
	require.True(t, ok)

	ok = ts.AddTableRecord(&TableRecord{
		TableID:   3,
		CaptureID: "capture-2",
		Status:    0,
	})
	require.True(t, ok)

	ok = ts.AddTableRecord(&TableRecord{
		TableID:   4,
		CaptureID: "capture-2",
		Status:    0,
	})
	require.True(t, ok)

	ok = ts.AddTableRecord(&TableRecord{
		TableID:   5,
		CaptureID: "capture-3",
		Status:    0,
	})
	require.True(t, ok)

	require.Equal(t, 2, ts.CountTableByCaptureID("capture-1"))
	require.Equal(t, 2, ts.CountTableByCaptureID("capture-2"))
	require.Equal(t, 1, ts.CountTableByCaptureID("capture-3"))

	ok = ts.AddTableRecord(&TableRecord{
		TableID:   6,
		CaptureID: "capture-3",
		Status:    0,
	})
	require.True(t, ok)
	require.Equal(t, 2, ts.CountTableByCaptureID("capture-3"))

	captures := ts.GetDistinctCaptures()
	require.Len(t, captures, 3)
	require.Contains(t, captures, "capture-1")
	require.Contains(t, captures, "capture-2")
	require.Contains(t, captures, "capture-3")

	ok = ts.RemoveTableRecord(3)
	require.True(t, ok)
	ok = ts.RemoveTableRecord(4)
	require.True(t, ok)

	captures = ts.GetDistinctCaptures()
	require.Len(t, captures, 2)
	require.Contains(t, captures, "capture-1")
	require.Contains(t, captures, "capture-3")

	captureToTableMap := ts.GetAllTablesGroupedByCaptures()
	require.Equal(t, map[model.CaptureID]map[model.TableID]*TableRecord{
		"capture-1": {
			1: &TableRecord{
				TableID:   1,
				CaptureID: "capture-1",
				Status:    0,
			},
			2: &TableRecord{
				TableID:   2,
				CaptureID: "capture-1",
				Status:    0,
			},
		},
		"capture-3": {
			5: &TableRecord{
				TableID:   5,
				CaptureID: "capture-3",
				Status:    0,
			},
			6: &TableRecord{
				TableID:   6,
				CaptureID: "capture-3",
				Status:    0,
			},
		},
	}, captureToTableMap)

	ts.RemoveTableRecordByCaptureID("capture-3")
	_, ok = ts.GetTableRecord(5)
	require.False(t, ok)
	_, ok = ts.GetTableRecord(6)
	require.False(t, ok)

	allTables := ts.GetAllTables()
	require.Equal(t, map[model.TableID]*TableRecord{
		1: {
			TableID:   1,
			CaptureID: "capture-1",
			Status:    0,
		},
		2: {
			TableID:   2,
			CaptureID: "capture-1",
			Status:    0,
		},
	}, allTables)

	ok = ts.RemoveTableRecord(1)
	require.True(t, ok)
	ok = ts.RemoveTableRecord(2)
	require.True(t, ok)

	captureToTableMap = ts.GetAllTablesGroupedByCaptures()
	require.Len(t, captureToTableMap, 0)
}

func TestCountTableByStatus(t *testing.T) {
	ts := NewTableSet()
	ok := ts.AddTableRecord(&TableRecord{
		TableID:   1,
		CaptureID: "capture-1",
		Status:    1,
	})
	require.True(t, ok)

	ok = ts.AddTableRecord(&TableRecord{
		TableID:   2,
		CaptureID: "capture-1",
		Status:    2,
	})
	require.True(t, ok)

	ok = ts.AddTableRecord(&TableRecord{
		TableID:   3,
		CaptureID: "capture-2",
		Status:    3,
	})
	require.True(t, ok)

	ok = ts.AddTableRecord(&TableRecord{
		TableID:   4,
		CaptureID: "capture-2",
		Status:    1,
	})
	require.True(t, ok)

	ok = ts.AddTableRecord(&TableRecord{
		TableID:   5,
		CaptureID: "capture-3",
		Status:    2,
	})
	require.True(t, ok)

	require.Equal(t, 2, ts.CountTableByStatus(1))
	require.Equal(t, 2, ts.CountTableByStatus(2))
	require.Equal(t, 1, ts.CountTableByStatus(3))
}
