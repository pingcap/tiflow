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

package cdc

import (
	"context"
	"math"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/etcd"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

type taskSuite struct {
	s         *embed.Etcd
	c         *clientv3.Client
	w         *TaskWatcher
	endpoints []string
}

var _ = check.Suite(&taskSuite{})

func (s *taskSuite) SetUpTest(c *check.C) {
	dir := c.MkDir()
	url, etcd, err := etcd.SetupEmbedEtcd(dir)
	c.Assert(err, check.IsNil)

	endpoints := []string{url.String()}
	client, err := clientv3.New(clientv3.Config{
		Endpoints: endpoints,
	})
	c.Assert(err, check.IsNil)

	// Create a task watcher
	capture := &Capture{
		etcdClient: kv.NewCDCEtcdClient(context.TODO(), client),
		processors: make(map[string]*processor),
		info:       &model.CaptureInfo{ID: "task-suite-capture", AdvertiseAddr: "task-suite-addr"},
	}
	c.Assert(capture, check.NotNil)
	watcher := NewTaskWatcher(capture, &TaskWatcherConfig{
		Prefix: kv.TaskStatusKeyPrefix + "/" + capture.info.ID,
	})
	c.Assert(watcher, check.NotNil)

	s.s = etcd
	s.c = client
	s.w = watcher
	s.endpoints = endpoints
}
func (s *taskSuite) TearDownTest(c *check.C) {
	s.s.Close()
	s.c.Close()
}

func (s *taskSuite) TestNewTaskWatcher(c *check.C) {
	// Create a capture instance by initialize the struct,
	// NewCapture can not be used because it requires to
	// initialize the PD service witch does not support to
	// be embeded.
	capture := &Capture{
		etcdClient: kv.NewCDCEtcdClient(context.TODO(), s.c),
		processors: make(map[string]*processor),
		info:       &model.CaptureInfo{ID: "task-suite-capture", AdvertiseAddr: "task-suite-addr"},
	}
	c.Assert(capture, check.NotNil)
	c.Assert(NewTaskWatcher(capture, &TaskWatcherConfig{
		Prefix: kv.TaskStatusKeyPrefix + "/" + capture.info.ID,
	}), check.NotNil)
	capture.Close(context.Background())
}

func (s *taskSuite) setupFeedInfo(c *check.C, changeFeedID string) {
	client := kv.NewCDCEtcdClient(context.TODO(), s.c)
	// Create the change feed
	c.Assert(client.SaveChangeFeedInfo(s.c.Ctx(), &model.ChangeFeedInfo{
		SinkURI:    "mysql://fake",
		StartTs:    0,
		TargetTs:   math.MaxUint64,
		CreateTime: time.Now(),
	}, changeFeedID), check.IsNil)

	// Fake the change feed status
	c.Assert(client.PutChangeFeedStatus(s.c.Ctx(), changeFeedID,
		&model.ChangeFeedStatus{
			ResolvedTs:   1,
			CheckpointTs: 1,
		}), check.IsNil)
}
func (s *taskSuite) teardownFeedInfo(c *check.C, changeFeedID string) {
	etcd := s.c
	// Delete change feed info
	resp, err := etcd.Delete(s.c.Ctx(), kv.GetEtcdKeyChangeFeedInfo(changeFeedID), clientv3.WithPrefix())
	c.Assert(err, check.IsNil)
	c.Assert(resp, check.NotNil)

	// Delete change feed status(job status)
	resp, err = etcd.Delete(s.c.Ctx(), kv.GetEtcdKeyJob(changeFeedID), clientv3.WithPrefix())
	c.Assert(err, check.IsNil)
	c.Assert(resp, check.NotNil)
}

func (s *taskSuite) TestParseTask(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changeFeedID := "task-suite-changefeed"
	s.setupFeedInfo(c, changeFeedID)
	defer s.teardownFeedInfo(c, changeFeedID)

	tests := []struct {
		Desc     string
		Key      []byte
		Expected *Task
	}{
		{"nil task key", nil, nil},
		{"short task key", []byte("test"), nil},
		{"normal task key",
			[]byte(kv.GetEtcdKeyTaskStatus(changeFeedID, s.w.capture.info.ID)),
			&Task{changeFeedID, 1}},
	}
	for _, t := range tests {
		c.Log("testing ", t.Desc)
		task, err := s.w.parseTask(ctx, t.Key, nil)
		if t.Expected == nil {
			c.Assert(err, check.NotNil)
			c.Assert(task, check.IsNil)
		} else {
			c.Assert(task, check.DeepEquals, t.Expected)
		}
	}
}

func (s *taskSuite) TestWatch(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.setupFeedInfo(c, "changefeed-1")
	defer s.teardownFeedInfo(c, "changefeed-1")

	client := kv.NewCDCEtcdClient(context.TODO(), s.c)
	// Watch with a canceled context
	failedCtx, cancel := context.WithCancel(context.Background())
	cancel()
	ev := <-s.w.Watch(failedCtx)
	if ev != nil {
		c.Assert(ev.Err, check.NotNil)
	}

	// Watch with a normal context
	ch := s.w.Watch(context.Background())

	// Trigger the ErrCompacted error
	c.Assert(failpoint.Enable("github.com/pingcap/ticdc/cdc.restart_task_watch", "50%off"), check.IsNil)

	// Put task changefeed-1
	c.Assert(client.PutTaskStatus(s.c.Ctx(), "changefeed-1",
		s.w.capture.info.ID,
		&model.TaskStatus{}), check.IsNil)
	ev = <-ch
	c.Assert(len(ch), check.Equals, 0)
	c.Assert(ev, check.NotNil)
	c.Assert(ev.Err, check.IsNil)
	c.Assert(ev.Op, check.Equals, TaskOpCreate)
	c.Assert(ev.Task.ChangeFeedID, check.Equals, "changefeed-1")
	c.Assert(ev.Task.CheckpointTS, check.Equals, uint64(1))

	// Stop the task changefeed-1
	c.Assert(client.PutTaskStatus(s.c.Ctx(), "changefeed-1",
		s.w.capture.info.ID,
		&model.TaskStatus{AdminJobType: model.AdminStop}), check.IsNil)
	ev = <-ch
	c.Assert(len(ch), check.Equals, 0)
	c.Assert(ev, check.NotNil)
	c.Assert(ev.Err, check.IsNil)
	c.Assert(ev.Op, check.Equals, TaskOpDelete)
	c.Assert(ev.Task.ChangeFeedID, check.Equals, "changefeed-1")
	c.Assert(ev.Task.CheckpointTS, check.Equals, uint64(1))

	// Resume the task changefeed-1
	c.Assert(client.PutTaskStatus(s.c.Ctx(), "changefeed-1",
		s.w.capture.info.ID,
		&model.TaskStatus{AdminJobType: model.AdminResume}), check.IsNil)
	ev = <-ch
	c.Assert(len(ch), check.Equals, 0)
	c.Assert(ev, check.NotNil)
	c.Assert(ev.Err, check.IsNil)
	c.Assert(ev.Op, check.Equals, TaskOpCreate)
	c.Assert(ev.Task.ChangeFeedID, check.Equals, "changefeed-1")
	c.Assert(ev.Task.CheckpointTS, check.Equals, uint64(1))

	// Delete the task changefeed-1
	c.Assert(client.DeleteTaskStatus(ctx, "changefeed-1",
		s.w.capture.info.ID), check.IsNil)
	ev = <-ch
	c.Assert(len(ch), check.Equals, 0)
	c.Assert(ev, check.NotNil)
	c.Assert(ev.Err, check.IsNil)
	c.Assert(ev.Op, check.Equals, TaskOpDelete)
	c.Assert(ev.Task.ChangeFeedID, check.Equals, "changefeed-1")
	c.Assert(ev.Task.CheckpointTS, check.Equals, uint64(1))

	// Put task changefeed-2 which does not exist
	c.Assert(client.PutTaskStatus(s.c.Ctx(), "changefeed-2",
		s.w.capture.info.ID,
		&model.TaskStatus{}), check.IsNil)
	c.Assert(len(ch), check.Equals, 0)
}

func (s *taskSuite) TestRebuildTaskEvents(c *check.C) {
	type T map[string]*TaskEvent
	tests := []struct {
		desc     string
		outdated T
		latest   T
		expected T
	}{
		{
			desc:     "nil outdated",
			outdated: nil,
			latest:   T{"changefeed-1": &TaskEvent{TaskOpCreate, &Task{"changeed-1", 0}, nil}},
			expected: T{"changefeed-1": &TaskEvent{TaskOpCreate, &Task{"changeed-1", 0}, nil}},
		},
		{
			desc:     "empty outdated",
			outdated: nil,
			latest:   T{"changefeed-1": &TaskEvent{TaskOpCreate, &Task{"changeed-1", 0}, nil}},
			expected: T{"changefeed-1": &TaskEvent{TaskOpCreate, &Task{"changeed-1", 0}, nil}},
		},
		{
			desc:     "need to be updated",
			outdated: T{"changefeed-1": &TaskEvent{TaskOpCreate, &Task{"changeed-1", 0}, nil}},
			latest:   T{"changefeed-1": &TaskEvent{TaskOpDelete, &Task{"changeed-1", 0}, nil}},
			expected: T{"changefeed-1": &TaskEvent{TaskOpDelete, &Task{"changeed-1", 0}, nil}},
		},
		{
			desc:     "miss some events",
			outdated: T{"changefeed-1": &TaskEvent{TaskOpCreate, &Task{"changeed-1", 0}, nil}},
			latest: T{
				"changefeed-1": &TaskEvent{TaskOpDelete, &Task{"changeed-1", 0}, nil},
				"changefeed-2": &TaskEvent{TaskOpCreate, &Task{"changefeed-2", 0}, nil},
			},
			expected: T{
				"changefeed-1": &TaskEvent{TaskOpDelete, &Task{"changeed-1", 0}, nil},
				"changefeed-2": &TaskEvent{TaskOpCreate, &Task{"changefeed-2", 0}, nil},
			},
		},
		{
			desc: "left some events",
			outdated: T{
				"changefeed-1": &TaskEvent{TaskOpDelete, &Task{"changeed-1", 0}, nil},
				"changefeed-2": &TaskEvent{TaskOpCreate, &Task{"changefeed-2", 0}, nil},
			},
			latest: T{"changefeed-1": &TaskEvent{TaskOpCreate, &Task{"changeed-1", 0}, nil}},
			expected: T{
				"changefeed-1": &TaskEvent{TaskOpCreate, &Task{"changeed-1", 0}, nil},
				"changefeed-2": &TaskEvent{TaskOpDelete, &Task{"changefeed-2", 0}, nil},
			},
		},
	}

	for _, t := range tests {
		c.Log("RUN CASE: ", t.desc)
		s.w.events = t.outdated
		got := s.w.rebuildTaskEvents(t.latest)
		c.Assert(len(got), check.Equals, len(t.expected))
		for k, v := range got {
			e := t.expected[k]
			c.Assert(v.Err, check.IsNil)
			c.Assert(v.Op, check.Equals, e.Op)
			c.Assert(v.Task, check.DeepEquals, e.Task)
		}
	}
}
