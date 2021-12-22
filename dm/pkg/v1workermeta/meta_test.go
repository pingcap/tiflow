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

	. "github.com/pingcap/check"
	"github.com/syndtr/goleveldb/leveldb"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testMeta struct{}

var _ = Suite(&testMeta{})

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

func testSetUpDB(c *C) *leveldb.DB {
	c.Assert(testTask1.Adjust(true), IsNil)
	c.Assert(testTask2.Adjust(true), IsNil)

	testTask1Str, err := testTask1.Toml()
	c.Assert(err, IsNil)
	testTask1Meta = &pb.V1SubTaskMeta{
		Op:    pb.TaskOp_Start,
		Name:  testTask1.Name,
		Stage: pb.Stage_New,
		Task:  []byte(testTask1Str),
	}

	testTask2Str, err := testTask2.Toml()
	c.Assert(err, IsNil)
	testTask2Meta = &pb.V1SubTaskMeta{
		Op:    pb.TaskOp_Start,
		Name:  testTask2.Name,
		Stage: pb.Stage_New,
		Task:  []byte(testTask2Str),
	}

	dir := c.MkDir()
	dbDir := path.Join(dir, "kv")
	db, err := openDB(dbDir, defaultKVConfig)
	if err != nil {
		c.Fatalf("fail to open leveldb %v", err)
	}

	return db
}

func (t *testMeta) TestNewMetaDB(c *C) {
	db := testSetUpDB(c)
	defer db.Close()

	metaDB, err := newMeta(db)
	c.Assert(err, IsNil)
	c.Assert(metaDB.tasks, HasLen, 0)

	// check nil db
	metaDB, err = newMeta(nil)
	c.Assert(terror.ErrWorkerLogInvalidHandler.Equal(err), IsTrue)
	c.Assert(metaDB, IsNil)
}

func (t *testMeta) TestTask(c *C) {
	db := testSetUpDB(c)
	defer db.Close()

	// set task meta
	c.Assert(terror.ErrWorkerLogInvalidHandler.Equal(setTaskMeta(nil, nil)), IsTrue)
	err := setTaskMeta(db, nil)
	c.Assert(err, ErrorMatches, ".*empty task.*")

	err = setTaskMeta(db, &pb.V1SubTaskMeta{})
	c.Assert(err, ErrorMatches, ".*empty task.*")

	c.Assert(setTaskMeta(db, testTask1Meta), IsNil)
	c.Assert(setTaskMeta(db, testTask2Meta), IsNil)

	// load task meta
	metaDB, err := newMeta(db)
	c.Assert(err, IsNil)
	c.Assert(metaDB.tasks, DeepEquals, map[string]*pb.V1SubTaskMeta{
		"task1": testTask1Meta,
		"task2": testTask2Meta,
	})

	// delete task meta
	c.Assert(deleteTaskMeta(db, "task1"), IsNil)

	// load task meta
	metaDB, err = newMeta(db)
	c.Assert(err, IsNil)
	c.Assert(metaDB.tasks, DeepEquals, map[string]*pb.V1SubTaskMeta{
		"task2": testTask2Meta,
	})

	// delete task meta
	c.Assert(deleteTaskMeta(db, "task2"), IsNil)

	// load task meta
	metaDB, err = newMeta(db)
	c.Assert(err, IsNil)
	c.Assert(metaDB.tasks, HasLen, 0)
}
