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

package checkpoints

import (
	"container/heap"
	"context"
	"encoding/json"
	"io"
	"os"
	"path"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
	"github.com/pingcap/tiflow/sync_diff_inspector/config"
	"github.com/pingcap/tiflow/sync_diff_inspector/report"
	"go.uber.org/zap"
)

// Write file to temp and atomically move when everything else succeeds.
func writeFileAtomic(filename string, data []byte, perm os.FileMode) error {
	dir, name := path.Dir(filename), path.Base(filename)
	f, err := os.CreateTemp(dir, name)
	if err != nil {
		return err
	}
	n, err := f.Write(data)
	f.Close()
	if err == nil && n < len(data) {
		err = io.ErrShortWrite
	} else {
		err = os.Chmod(f.Name(), perm)
	}
	if err != nil {
		os.Remove(f.Name())
		return err
	}
	return os.Rename(f.Name(), filename)
}

const (
	// SuccessState means
	// for chunk: this chunk's data is equal
	// for table: means this all chunk in this table is equal(except ignore chunk)
	SuccessState = "success"

	// FailedState means
	// for chunk: this chunk's data is not equal
	// for table: some chunks' data is not equal or some chunk check failed in this table
	FailedState = "failed"

	// IgnoreState means
	// for chunk: this chunk is ignored. if it is Empty chunk, will ignore some chunk
	// for table: don't have this state
	IgnoreState = "ignore"
)

// Node is the struct for node
type Node struct {
	State string `json:"state"` // indicate the state ("success" or "failed") of the chunk

	ChunkRange *chunk.Range `json:"chunk-range"`
	IndexID    int64        `json:"index-id"`
}

// GetID returns id from the node
func (n *Node) GetID() *chunk.CID { return n.ChunkRange.Index }

// GetState returns the state from the node
func (n *Node) GetState() string { return n.State }

// GetTableIndex returns table index
func (n *Node) GetTableIndex() int { return n.ChunkRange.Index.TableIndex }

// GetBucketIndexLeft returns BucketIndexLeft
func (n *Node) GetBucketIndexLeft() int { return n.ChunkRange.Index.BucketIndexLeft }

// GetBucketIndexRight returns BucketIndexRight
func (n *Node) GetBucketIndexRight() int { return n.ChunkRange.Index.BucketIndexRight }

// GetChunkIndex returns ChunkIndex
func (n *Node) GetChunkIndex() int { return n.ChunkRange.Index.ChunkIndex }

// IsAdjacent represents whether the next node is adjacent node.
// it's the important logic for checkpoint update.
// we need keep this node save to checkpoint in global order.
func (n *Node) IsAdjacent(next *Node) bool {
	if n.GetTableIndex() == next.GetTableIndex()-1 {
		if n.ChunkRange.IsLastChunkForTable() && next.ChunkRange.IsFirstChunkForTable() {
			return true
		}
		return false
	}
	if n.GetTableIndex() == next.GetTableIndex() {
		// same table
		if n.GetBucketIndexRight() == next.GetBucketIndexLeft()-1 {
			if n.ChunkRange.IsLastChunkForBucket() && next.ChunkRange.IsFirstChunkForBucket() {
				return true
			}
			return false
		}
		if n.GetBucketIndexLeft() == next.GetBucketIndexLeft() {
			return n.GetChunkIndex() == next.GetChunkIndex()-1
		}
		return false
	}
	return false
}

// IsLess represents whether the cur node is less than next node.
func (n *Node) IsLess(next *Node) bool {
	if n.GetTableIndex() < next.GetTableIndex() {
		return true
	}
	if n.GetTableIndex() == next.GetTableIndex() {
		if n.GetBucketIndexLeft() <= next.GetBucketIndexLeft()-1 {
			return true
		}
		if n.GetBucketIndexLeft() == next.GetBucketIndexLeft() {
			return n.GetChunkIndex() < next.GetChunkIndex()
		}
		return false
	}
	return false
}

// heap maintain a Min Heap, which can be accessed by multiple threads and protected by mutex.
type nodeHeap struct {
	Nodes            []*Node
	CurrentSavedNode *Node       // CurrentSavedNode save the minimum checker chunk, updated by `GetChunkSnapshot` method
	mu               *sync.Mutex // protect critical section
}

// Checkpoint provide the ability to restart the sync-diff process from the
// latest previous exit point (due to error or intention).
type Checkpoint struct {
	hp *nodeHeap
}

// SavedState contains the information of the latest checked chunk and state of `report`
// When sync-diff start from the checkpoint, it will load this information and continue running
type SavedState struct {
	Chunk  *Node          `json:"chunk-info"`
	Report *report.Report `json:"report-info"`
}

// InitCurrentSavedID the method is only used in initialization without lock, be cautious
func (cp *Checkpoint) InitCurrentSavedID(n *Node) {
	cp.hp.CurrentSavedNode = n
}

// GetCurrentSavedID returns the saved id with lock
func (cp *Checkpoint) GetCurrentSavedID() *Node {
	cp.hp.mu.Lock()
	defer cp.hp.mu.Unlock()
	return cp.hp.CurrentSavedNode
}

// Insert inserts a new node
func (cp *Checkpoint) Insert(node *Node) {
	cp.hp.mu.Lock()
	heap.Push(cp.hp, node)
	cp.hp.mu.Unlock()
}

// Len gets the length of the heap
func (hp *nodeHeap) Len() int { return len(hp.Nodes) }

// Less determines which is more priority than another
func (hp *nodeHeap) Less(i, j int) bool {
	return hp.Nodes[i].IsLess(hp.Nodes[j])
}

// Swap implementation of swap for the heap interface
func (hp *nodeHeap) Swap(i, j int) {
	hp.Nodes[i], hp.Nodes[j] = hp.Nodes[j], hp.Nodes[i]
}

// Push implementation of push for the heap interface
func (hp *nodeHeap) Push(x interface{}) {
	hp.Nodes = append(hp.Nodes, x.(*Node))
}

// Pop implementation of pop for heap interface
func (hp *nodeHeap) Pop() (item interface{}) {
	if len(hp.Nodes) == 0 {
		return
	}

	hp.Nodes, item = hp.Nodes[:len(hp.Nodes)-1], hp.Nodes[len(hp.Nodes)-1]
	return
}

// Init initialize the Checkpoint
func (cp *Checkpoint) Init() {
	hp := &nodeHeap{
		mu:    &sync.Mutex{},
		Nodes: make([]*Node, 0),
		CurrentSavedNode: &Node{
			ChunkRange: &chunk.Range{
				Index:   chunk.GetInitCID(),
				IsFirst: true,
				IsLast:  true,
			},
		},
	}
	heap.Init(hp)
	cp.hp = hp
}

// GetChunkSnapshot get the snapshot of the minimum continuous checked chunk
func (cp *Checkpoint) GetChunkSnapshot() (cur *Node) {
	cp.hp.mu.Lock()
	defer cp.hp.mu.Unlock()
	for cp.hp.Len() != 0 && cp.hp.CurrentSavedNode.IsAdjacent(cp.hp.Nodes[0]) {
		cp.hp.CurrentSavedNode = heap.Pop(cp.hp).(*Node)
		cur = cp.hp.CurrentSavedNode
	}
	// wait for next 10s to check
	return cur
}

// SaveChunk saves the chunk to file.
func (cp *Checkpoint) SaveChunk(ctx context.Context, fileName string, cur *Node, reportInfo *report.Report) (*chunk.CID, error) {
	if cur == nil {
		return nil, nil
	}

	savedState := &SavedState{
		Chunk:  cur,
		Report: reportInfo,
	}
	checkpointData, err := json.Marshal(savedState)
	if err != nil {
		log.Warn("fail to save the chunk to the file", zap.Any("chunk index", cur.GetID()), zap.Error(err))
		return nil, errors.Trace(err)
	}

	if err = writeFileAtomic(fileName, checkpointData, config.LocalFilePerm); err != nil {
		return nil, err
	}
	log.Info("save checkpoint",
		zap.Any("chunk", cur),
		zap.String("state", cur.GetState()))
	return cur.GetID(), nil
}

// LoadChunk loads chunk info from file `chunk`
func (cp *Checkpoint) LoadChunk(fileName string) (*Node, *report.Report, error) {
	bytes, err := os.ReadFile(fileName)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	n := &SavedState{}
	err = json.Unmarshal(bytes, n)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return n.Chunk, n.Report, nil
}
