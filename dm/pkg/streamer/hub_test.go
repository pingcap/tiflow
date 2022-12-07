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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRelayLogInfo(t *testing.T) {
	t.Parallel()
	rli1 := RelayLogInfo{}
	rli1.SubDirSuffix = 1
	rli2 := RelayLogInfo{}
	rli2.SubDirSuffix = 2

	// compare by UUIDSuffix
	require.False(t, rli1.Earlier(&rli1))
	require.True(t, rli1.Earlier(&rli2))
	require.False(t, rli2.Earlier(&rli1))
	require.False(t, rli2.Earlier(&rli2))

	// compare by filename
	rli3 := RelayLogInfo{}
	rli3.SubDirSuffix = 1

	rli1.Filename = "mysql-bin.000001"
	rli3.Filename = "mysql-bin.000001" // equal
	require.False(t, rli1.Earlier(&rli3))
	require.False(t, rli3.Earlier(&rli1))

	rli3.Filename = "mysql-bin.000003"
	require.True(t, rli1.Earlier(&rli3))
	require.False(t, rli3.Earlier(&rli1))

	// string representation
	rli3.SubDir = "c6ae5afe-c7a3-11e8-a19d-0242ac130006.000001"
	require.Equal(t, filepath.Join(rli3.SubDir, rli3.Filename), rli3.String())
}

func TestRelayLogInfoHub(t *testing.T) {
	t.Parallel()
	rlih := newRelayLogInfoHub()
	taskName, earliest := rlih.earliest()
	require.Equal(t, "", taskName)
	require.Nil(t, earliest)

	// update with invalid args
	err := rlih.update("invalid-task", "invalid.uuid", "mysql-bin.000006")
	require.Error(t, err)
	err = rlih.update("invalid-task", "c6ae5afe-c7a3-11e8-a19d-0242ac130006.000002", "invalid.binlog")
	require.Error(t, err)
	taskName, earliest = rlih.earliest()
	require.Equal(t, "", taskName)
	require.Nil(t, earliest)

	cases := []struct {
		taskName string
		uuid     string
		filename string
	}{
		{"task-1", "c6ae5afe-c7a3-11e8-a19d-0242ac130006.000004", "mysql-bin.000001"},
		{"task-2", "e9540a0d-f16d-11e8-8cb7-0242ac130008.000003", "mysql-bin.000002"},
		{"task-3", "c6ae5afe-c7a3-11e8-a19d-0242ac130006.000002", "mysql-bin.000003"},
		{"task-3", "c6ae5afe-c7a3-11e8-a19d-0242ac130006.000002", "mysql-bin.000002"},
	}

	// update from later to earlier
	for _, cs := range cases {
		err = rlih.update(cs.taskName, cs.uuid, cs.filename)
		require.NoError(t, err)
		taskName, earliest = rlih.earliest()
		require.Equal(t, cs.taskName, taskName)
		require.Equal(t, cs.uuid, earliest.SubDir)
		require.Equal(t, cs.filename, earliest.Filename)
	}
	require.Len(t, rlih.logs, 3)

	// remove earliest
	cs := cases[3]
	rlih.remove(cs.taskName)
	require.Len(t, rlih.logs, 2)

	taskName, earliest = rlih.earliest()
	cs = cases[1]
	require.Equal(t, cs.taskName, taskName)
	require.Equal(t, cs.uuid, earliest.SubDir)
	require.Equal(t, cs.filename, earliest.Filename)

	// remove non-earliest
	cs = cases[0]
	rlih.remove(cs.taskName)
	require.Len(t, rlih.logs, 1)

	taskName, earliest = rlih.earliest()
	cs = cases[1]
	require.Equal(t, cs.taskName, taskName)
	require.Equal(t, cs.uuid, earliest.SubDir)
	require.Equal(t, cs.filename, earliest.Filename)

	// all removed
	cs = cases[1]
	rlih.remove(cs.taskName)
	require.Len(t, rlih.logs, 0)

	taskName, earliest = rlih.earliest()
	require.Equal(t, "", taskName)
	require.Nil(t, earliest)
}

func TestReaderHub(t *testing.T) {
	t.Parallel()
	h := GetReaderHub()
	require.NotNil(t, h)

	h2 := GetReaderHub()
	require.NotNil(t, h2)
	require.Equal(t, h, h2)

	// no earliest
	erli := h.EarliestActiveRelayLog()
	require.Nil(t, erli)

	// update one
	err := h.UpdateActiveRelayLog("task-1", "c6ae5afe-c7a3-11e8-a19d-0242ac130006.000004", "mysql-bin.000001")
	require.NoError(t, err)

	// the only one is the earliest
	erli = h.EarliestActiveRelayLog()
	require.NotNil(t, erli)
	require.Equal(t, "c6ae5afe-c7a3-11e8-a19d-0242ac130006.000004", erli.SubDir)
	require.Equal(t, "mysql-bin.000001", erli.Filename)

	// update an earlier one
	err = h.UpdateActiveRelayLog("task-2", "c6ae5afe-c7a3-11e8-a19d-0242ac130006.000002", "mysql-bin.000002")
	require.NoError(t, err)

	// the earlier one is the earliest
	erli = h.EarliestActiveRelayLog()
	require.NotNil(t, erli)
	require.Equal(t, "c6ae5afe-c7a3-11e8-a19d-0242ac130006.000002", erli.SubDir)
	require.Equal(t, "mysql-bin.000002", erli.Filename)

	// remove the earlier one
	h.RemoveActiveRelayLog("task-2")

	// the only one is the earliest
	erli = h.EarliestActiveRelayLog()
	require.NotNil(t, erli)
	require.Equal(t, "c6ae5afe-c7a3-11e8-a19d-0242ac130006.000004", erli.SubDir)
	require.Equal(t, "mysql-bin.000001", erli.Filename)

	// remove the only one
	h.RemoveActiveRelayLog("task-1")

	// no earliest
	erli = h.EarliestActiveRelayLog()
	require.Nil(t, erli)
}
