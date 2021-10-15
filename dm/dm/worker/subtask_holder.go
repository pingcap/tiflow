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

package worker

import (
	"context"
	"sync"
)

// subTaskHolder holds subtask instances.
type subTaskHolder struct {
	// protect concurrent access of `subTasks`
	// the internal operation of the subtask should be protected by any mechanism in the subtask itself.
	mu       sync.RWMutex
	subTasks map[string]*SubTask
}

// newSubTaskHolder creates a new subTaskHolder instance.
func newSubTaskHolder() *subTaskHolder {
	return &subTaskHolder{
		subTasks: make(map[string]*SubTask, 5),
	}
}

// recordSubTask records subtask instance.
func (h *subTaskHolder) recordSubTask(st *SubTask) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.subTasks[st.cfg.Name] = st
}

// removeSubTask removes subtask instance.
func (h *subTaskHolder) removeSubTask(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.subTasks, name)
}

// resetAllSubTasks does Close, change cfg.UseRelay then Init the subtasks.
func (h *subTaskHolder) resetAllSubTasks(useRelay bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, st := range h.subTasks {
		stage := st.Stage()
		st.Close()
		// TODO: make a st.reset
		st.ctx, st.cancel = context.WithCancel(context.Background())
		st.cfg.UseRelay = useRelay
		st.Run(stage)
	}
}

// closeAllSubTasks closes all subtask instances.
func (h *subTaskHolder) closeAllSubTasks() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, st := range h.subTasks {
		st.Close()
	}
	h.subTasks = make(map[string]*SubTask)
}

// findSubTask finds subtask instance by name.
func (h *subTaskHolder) findSubTask(name string) *SubTask {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.subTasks[name]
}

// getAllSubTasks returns all subtask instances.
func (h *subTaskHolder) getAllSubTasks() map[string]*SubTask {
	h.mu.RLock()
	defer h.mu.RUnlock()
	result := make(map[string]*SubTask, len(h.subTasks))
	for name, st := range h.subTasks {
		result[name] = st
	}
	return result
}
