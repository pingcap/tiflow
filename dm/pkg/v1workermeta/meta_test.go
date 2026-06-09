// Copyright 2020 PingCAP, Inc.
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

package v1workermeta

import (
	"path"
	"testing"

	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
	"github.com/syndtr/goleveldb/leveldb"
)

var (
	testTask1 = &config.SubTaskConfig{
		Name:     "task1",
		SourceID: "replica-1",
	}
	testTask1Meta *pb.V1SubTaskMeta

	testTask2 = &config.SubTaskConfig{
		Name:     "task2",
		SourceID: "replica-1",
	}
	testTask2Meta *pb.V1SubTaskMeta
)

func testSetUpDB(t *testing.T) *leveldb.DB {
	require.NoError(t, testTask1.Adjust(true))
	require.NoError(t, testTask2.Adjust(true))

	testTask1Str, err := testTask1.Toml()
	require.NoError(t, err)
	testTask1Meta = &pb.V1SubTaskMeta{
		Op:    pb.TaskOp_Start,
		Name:  testTask1.Name,
		Stage: pb.Stage_New,
		Task:  []byte(testTask1Str),
	}

	testTask2Str, err := testTask2.Toml()
	require.NoError(t, err)
	testTask2Meta = &pb.V1SubTaskMeta{
		Op:    pb.TaskOp_Start,
		Name:  testTask2.Name,
		Stage: pb.Stage_New,
		Task:  []byte(testTask2Str),
	}

	dir := t.TempDir()
	dbDir := path.Join(dir, "kv")
	db, err := openDB(dbDir, defaultKVConfig)
	if err != nil {
		t.Fatalf("fail to open leveldb %v", err)
	}

	return db
}

func TestNewMetaDB(t *testing.T) {
	db := testSetUpDB(t)
	defer db.Close()

	metaDB, err := newMeta(db)
	require.NoError(t, err)
	require.Len(t, metaDB.tasks, 0)

	// check nil db
	metaDB, err = newMeta(nil)
	require.True(t, terror.ErrWorkerLogInvalidHandler.Equal(err))
	require.Nil(t, metaDB)
}

func TestTask(t *testing.T) {
	db := testSetUpDB(t)
	defer db.Close()

	// set task meta
	require.True(t, terror.ErrWorkerLogInvalidHandler.Equal(setTaskMeta(nil, nil)))
	err := setTaskMeta(db, nil)
	require.Error(t, err)
	require.Regexp(t, ".*empty task.*", err.Error())

	err = setTaskMeta(db, &pb.V1SubTaskMeta{})
	require.Error(t, err)
	require.Regexp(t, ".*empty task.*", err.Error())

	require.NoError(t, setTaskMeta(db, testTask1Meta))
	require.NoError(t, setTaskMeta(db, testTask2Meta))

	// load task meta
	metaDB, err := newMeta(db)
	require.NoError(t, err)
	require.Equal(t, map[string]*pb.V1SubTaskMeta{
		"task1": testTask1Meta,
		"task2": testTask2Meta,
	}, metaDB.tasks)

	// delete task meta
	require.NoError(t, deleteTaskMeta(db, "task1"))

	// load task meta
	metaDB, err = newMeta(db)
	require.NoError(t, err)
	require.Equal(t, map[string]*pb.V1SubTaskMeta{
		"task2": testTask2Meta,
	}, metaDB.tasks)

	// delete task meta
	require.NoError(t, deleteTaskMeta(db, "task2"))

	// load task meta
	metaDB, err = newMeta(db)
	require.NoError(t, err)
	require.Len(t, metaDB.tasks, 0)
}
