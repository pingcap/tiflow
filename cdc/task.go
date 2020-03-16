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
	"errors"
	"strings"

	"github.com/pingcap/log"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// TaskEventOp is the operation of a task
type TaskEventOp string

// Task Event Operatrions
const (
	TaskOpCreate = "create"
	TaskOpDelete = "delete"
)

// Task is dispatched by the owner
type Task struct {
	ChangeFeedID string
	CheckpointTS uint64
}

// TaskEvent represents a task is created or deleted
type TaskEvent struct {
	Op   TaskEventOp
	Task *Task
	Err  error
}

// TaskWatcher watches on new tasks
type TaskWatcher struct {
	capture *Capture
	cfg     *TaskWatcherConfig

	tasks []*Task
}

// TaskWatcherConfig configures a watcher
type TaskWatcherConfig struct {
	Prefix      string
	ChannelSize int64
}

// NewTaskWatcher returns a TaskWatcher
func NewTaskWatcher(c *Capture, cfg *TaskWatcherConfig) *TaskWatcher {
	return &TaskWatcher{capture: c, cfg: cfg}
}

// Watch on the new tasks, a channel is returned
func (w *TaskWatcher) Watch(ctx context.Context) <-chan *TaskEvent {
	c := make(chan *TaskEvent, w.cfg.ChannelSize)
	go w.watch(ctx, c)
	return c
}

func (w *TaskWatcher) watch(ctx context.Context, c chan *TaskEvent) {
	etcd := w.capture.etcdClient.Client

	// Leader is required in this context to prevent read outdated data
	// from a stale leader
	ctx = clientv3.WithRequireLeader(ctx)

	// Send a task event to the channel, checks ctx.Done() to avoid blocking
	send := func(ctx context.Context, ev *TaskEvent) {
		select {
		case <-ctx.Done():
			close(c)
		case c <- ev:
		}
	}
restart:
	// Load all the existed tasks
	var tasks []*Task
	resp, err := etcd.Get(ctx, w.cfg.Prefix, clientv3.WithPrefix())
	if err != nil {
		send(ctx, &TaskEvent{Err: err})
		return
	}
	for _, kv := range resp.Kvs {
		task, err := w.parseTask(ctx, kv.Key, kv.Value)
		if err != nil {
			log.Warn("parse task failed",
				zap.String("captureid", w.capture.info.ID),
				zap.Error(err))
			continue
		}
		if task == nil {
			continue
		}
		tasks = append(tasks, task)
	}

	// Rebuild the missed events
	// When an error is occured during watch, the watch routine is restarted,
	// in that case, some events maybe missed. Rebuild the events by comparing
	// the new task list with the last successfully recorded tasks.
	events := w.rebuildTaskEvents(tasks)
	for _, ev := range events {
		send(ctx, ev)
	}

	wch := etcd.Watch(ctx, w.cfg.Prefix,
		clientv3.WithPrefix(),
		clientv3.WithPrevKV(),
		clientv3.WithRev(resp.Header.Revision))
	for wresp := range wch {
		if wresp.Err() != nil {
			goto restart
		}
		for _, ev := range wresp.Events {
			if ev.IsCreate() { // a key is new created
				task, err := w.parseTask(ctx, ev.Kv.Key, ev.Kv.Value)
				if err != nil {
					log.Warn("parse task failed",
						zap.String("captureid", w.capture.info.ID),
						zap.Error(err))
					continue
				}
				if task == nil {
					continue
				}
				send(ctx, &TaskEvent{Op: TaskOpCreate, Task: task})
			} else if ev.Type == clientv3.EventTypeDelete {
				task, err := w.parseTask(ctx, ev.PrevKv.Key, ev.PrevKv.Value)
				if err != nil {
					log.Warn("parse task failed",
						zap.String("captureid", w.capture.info.ID),
						zap.Error(err))
					continue
				}
				if task == nil {
					continue
				}
				send(ctx, &TaskEvent{Op: TaskOpDelete, Task: task})
			}
		}
	}

}

func (w *TaskWatcher) parseTask(ctx context.Context,
	key, val []byte) (*Task, error) {
	if len(key) <= len(w.cfg.Prefix) {
		return nil, errors.New("invalid task key: " + string(key))
	}
	remain := string(key[len(w.cfg.Prefix)+1:])
	parts := strings.Split(remain, "/")
	if len(parts) < 2 {
		return nil, errors.New("invalid task key layout" + string(key))
	}

	changeFeedID, kind := parts[0], parts[1]
	if kind != "status" {
		return nil, nil
	}

	cf, err := w.capture.etcdClient.GetChangeFeedInfo(ctx, changeFeedID)
	if err != nil {
		return nil, err
	}
	status, err := w.capture.etcdClient.GetChangeFeedStatus(ctx, changeFeedID)
	if err != nil {
		return nil, err
	}
	checkpointTs := cf.GetCheckpointTs(status)
	return &Task{ChangeFeedID: changeFeedID, CheckpointTS: checkpointTs}, nil
}

func (w *TaskWatcher) rebuildTaskEvents(tasks []*Task) []*TaskEvent {
	var events []*TaskEvent

	latest := make(map[string]*Task)
	for _, task := range tasks {
		latest[task.ChangeFeedID] = task
	}

	outdated := make(map[string]*Task)
	for _, task := range w.tasks {
		outdated[task.ChangeFeedID] = task

		// Check if the task still exists
		if _, ok := latest[task.ChangeFeedID]; !ok {
			events = append(events, &TaskEvent{Op: TaskOpDelete, Task: task})
		}
	}

	for _, task := range tasks {
		if _, ok := outdated[task.ChangeFeedID]; !ok {
			events = append(events, &TaskEvent{Op: TaskOpCreate, Task: task})
		}
	}

	// Update to the latest tasks
	w.tasks = tasks

	return events
}
