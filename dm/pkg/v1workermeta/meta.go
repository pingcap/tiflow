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
	"github.com/golang/protobuf/proto"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// meta stores metadata of tasks.
type meta struct {
	tasks map[string]*pb.V1SubTaskMeta
}

// newMeta returns a meta object.
func newMeta(db *leveldb.DB) (*meta, error) {
	tasks, err := loadTaskMetas(db)
	if err != nil {
		return nil, err
	}

	return &meta{
		tasks: tasks,
	}, nil
}

// TasksMeta returns meta of all tasks.
func (meta *meta) TasksMeta() map[string]*pb.V1SubTaskMeta {
	tasks := make(map[string]*pb.V1SubTaskMeta, len(meta.tasks))
	for name, task := range meta.tasks {
		tasks[name] = proto.Clone(task).(*pb.V1SubTaskMeta)
	}
	return tasks
}

// taskMetaPrefix is prefix of task meta key.
var taskMetaPrefix = []byte("!DM!TaskMeta")

// encodeTaskMetaKey encodes take name into a task meta key.
func encodeTaskMetaKey(name string) []byte {
	key := append([]byte{}, taskMetaPrefix...)
	return append(key, name...)
}

// loadTaskMetas loads all task metas from kv db.
func loadTaskMetas(db *leveldb.DB) (map[string]*pb.V1SubTaskMeta, error) {
	if db == nil {
		return nil, terror.ErrWorkerLogInvalidHandler.Generate()
	}

	var (
		tasks = make(map[string]*pb.V1SubTaskMeta)
		err   error
	)

	iter := db.NewIterator(util.BytesPrefix(taskMetaPrefix), nil)
	for iter.Next() {
		taskBytes := iter.Value()
		task := &pb.V1SubTaskMeta{}
		err = task.Unmarshal(taskBytes)
		if err != nil {
			iter.Release()
			return nil, terror.ErrWorkerLogUnmarshalTaskMeta.Delegate(err, taskBytes)
		}

		tasks[task.Name] = task
	}
	iter.Release()

	err = iter.Error()
	if err != nil {
		return nil, terror.ErrWorkerLogFetchTaskFromMeta.Delegate(err, taskMetaPrefix)
	}

	return tasks, nil
}

// setTaskMeta saves task meta into kv db.
// it's used for testing.
func setTaskMeta(db *leveldb.DB, task *pb.V1SubTaskMeta) error {
	if db == nil {
		return terror.ErrWorkerLogInvalidHandler.Generate()
	}

	err := verifyTaskMeta(task)
	if err != nil {
		return terror.Annotatef(err, "verify task meta %+v", task)
	}

	taskBytes, err := task.Marshal()
	if err != nil {
		return terror.ErrWorkerLogMarshalTask.Delegate(err, task)
	}

	err = db.Put(encodeTaskMetaKey(task.Name), taskBytes, nil)
	if err != nil {
		return terror.ErrWorkerLogSaveTaskMeta.Delegate(err, task.Name)
	}

	return nil
}

// deleteTaskMeta deletes task meta from kv DB.
// it's used for testing.
func deleteTaskMeta(db *leveldb.DB, name string) error {
	if db == nil {
		return terror.ErrWorkerLogInvalidHandler.Generate()
	}

	err := db.Delete(encodeTaskMetaKey(name), nil)
	if err != nil {
		return terror.ErrWorkerLogDeleteTaskMeta.Delegate(err, name)
	}

	return nil
}

// verifyTaskMeta verifies legality of take meta.
// it's used for testing.
func verifyTaskMeta(task *pb.V1SubTaskMeta) error {
	if task == nil {
		return terror.ErrWorkerLogVerifyTaskMeta.New("empty task not valid")
	}

	if len(task.Name) == 0 {
		return terror.ErrWorkerLogVerifyTaskMeta.New("empty task name not valid")
	}

	if len(task.Task) == 0 {
		return terror.ErrWorkerLogVerifyTaskMeta.New("empty task config not valid")
	}

	if task.Stage == pb.Stage_InvalidStage {
		return terror.ErrWorkerLogVerifyTaskMeta.Generatef("stage %s not valid", task.Stage)
	}

	if task.Op == pb.TaskOp_InvalidOp {
		return terror.ErrWorkerLogVerifyTaskMeta.Generatef("task operation %s not valid", task.Op)
	}

	return nil
}
