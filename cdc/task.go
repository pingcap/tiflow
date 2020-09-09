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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc"
	"go.uber.org/zap"
)

// TaskEventOp is the operation of a task
type TaskEventOp string

// Task Event Operatrions
const (
	TaskOpCreate TaskEventOp = "create"
	TaskOpDelete TaskEventOp = "delete"
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

	events map[string]*TaskEvent
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
	send := func(ctx context.Context, ev *TaskEvent) error {
		select {
		case <-ctx.Done():
			close(c)
			return ctx.Err()
		case c <- ev:
		}
		return nil
	}
restart:
	// Load all the existed tasks
	events := make(map[string]*TaskEvent)
	resp, err := etcd.Get(ctx, w.cfg.Prefix, clientv3.WithPrefix())
	if err != nil {
		_ = send(ctx, &TaskEvent{Err: err})
		return
	}
	for _, kv := range resp.Kvs {
		ev, err := w.parseTaskEvent(ctx, kv.Key, kv.Value)
		if err != nil {
			log.Warn("parse task event failed",
				zap.String("captureid", w.capture.info.ID),
				zap.Error(err))
			continue
		}
		events[ev.Task.ChangeFeedID] = ev
	}

	// Rebuild the missed events
	// When an error is occured during watch, the watch routine is restarted,
	// in that case, some events maybe missed. Rebuild the events by comparing
	// the new task list with the last successfully recorded tasks.
	events = w.rebuildTaskEvents(events)
	for _, ev := range events {
		if err := send(ctx, ev); err != nil {
			return
		}
	}

	wch := etcd.Watch(ctx, w.cfg.Prefix,
		clientv3.WithPrefix(),
		clientv3.WithPrevKV(),
		clientv3.WithRev(resp.Header.Revision+1))
	for wresp := range wch {
		err := wresp.Err()
		failpoint.Inject("restart-task-watch", func() {
			err = mvcc.ErrCompacted
		})
		if err != nil {
			goto restart
		}
		for _, ev := range wresp.Events {
			if ev.Type == clientv3.EventTypePut {
				ev, err := w.parseTaskEvent(ctx, ev.Kv.Key, ev.Kv.Value)
				if err != nil {
					log.Warn("parse task event failed",
						zap.String("captureid", w.capture.info.ID),
						zap.Error(err))
					continue
				}
				w.events[ev.Task.ChangeFeedID] = ev
				if err := send(ctx, ev); err != nil {
					return
				}
			} else if ev.Type == clientv3.EventTypeDelete {
				task, err := w.parseTask(ctx, ev.PrevKv.Key, ev.PrevKv.Value)
				if err != nil {
					log.Warn("parse task failed",
						zap.String("captureid", w.capture.info.ID),
						zap.Error(err))
					continue
				}
				delete(w.events, task.ChangeFeedID)
				if err := send(ctx, &TaskEvent{Op: TaskOpDelete, Task: task}); err != nil {
					return
				}
			}
		}
	}
	close(c)
}

func (w *TaskWatcher) parseTask(ctx context.Context,
	key, val []byte) (*Task, error) {
	if len(key) <= len(w.cfg.Prefix) {
		return nil, cerror.ErrInvalidTaskKey.GenWithStackByArgs(string(key))
	}
	changeFeedID := string(key[len(w.cfg.Prefix)+1:])
	cf, err := w.capture.etcdClient.GetChangeFeedInfo(ctx, changeFeedID)
	if err != nil {
		return nil, err
	}
	status, _, err := w.capture.etcdClient.GetChangeFeedStatus(ctx, changeFeedID)
	if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
		return nil, err
	}
	checkpointTs := cf.GetCheckpointTs(status)
	return &Task{ChangeFeedID: changeFeedID, CheckpointTS: checkpointTs}, nil
}

func (w *TaskWatcher) parseTaskEvent(ctx context.Context, key, val []byte) (*TaskEvent, error) {
	task, err := w.parseTask(ctx, key, val)
	if err != nil {
		log.Warn("parse task failed",
			zap.String("captureid", w.capture.info.ID),
			zap.Error(err))
		return nil, err
	}

	taskStatus := &model.TaskStatus{}
	if err := taskStatus.Unmarshal(val); err != nil {
		log.Warn("unmarshal task status failed",
			zap.String("captureid", w.capture.info.ID),
			zap.Error(err))
		return nil, err
	}
	var op TaskEventOp
	switch taskStatus.AdminJobType {
	case model.AdminNone, model.AdminResume:
		op = TaskOpCreate
	case model.AdminStop, model.AdminRemove, model.AdminFinish:
		op = TaskOpDelete
	}
	return &TaskEvent{Op: op, Task: task}, nil
}

func (w *TaskWatcher) rebuildTaskEvents(latest map[string]*TaskEvent) map[string]*TaskEvent {
	events := make(map[string]*TaskEvent)
	outdated := w.events
	for id, ev := range outdated {
		// Check if the task still exists
		if nev, ok := latest[id]; ok {
			if ev.Op != nev.Op {
				events[id] = nev
			}
		} else if ev.Op != TaskOpDelete {
			events[id] = &TaskEvent{Op: TaskOpDelete, Task: ev.Task}
		}
	}

	for id, ev := range latest {
		if _, ok := outdated[id]; !ok {
			events[id] = ev
		}
	}

	// Update to the latest tasks
	w.events = events

	return events
}
