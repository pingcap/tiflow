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

package tablepb

import (
	"bytes"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/pingcap/tiflow/cdc/model"
)

// Load TableState with THREAD-SAFE
func (s *TableState) Load() TableState {
	return TableState(atomic.LoadInt32((*int32)(s)))
}

// Store TableState with THREAD-SAFE
func (s *TableState) Store(new TableState) {
	atomic.StoreInt32((*int32)(s), int32(new))
}

// CompareAndSwap is just like sync/atomic.Atomic*.CompareAndSwap.
func (s *TableState) CompareAndSwap(old, new TableState) bool {
	oldx := int32(old)
	newx := int32(new)
	return atomic.CompareAndSwapInt32((*int32)(s), oldx, newx)
}

// TablePipeline is a pipeline which capture the change log from tikv in a table
type TablePipeline interface {
	// ID returns the ID of source table and mark table
	ID() (tableID int64)
	// Name returns the quoted schema and table name
	Name() string
	// ResolvedTs returns the resolved ts in this table pipeline
	ResolvedTs() model.Ts
	// CheckpointTs returns the checkpoint ts in this table pipeline
	CheckpointTs() model.Ts
	// UpdateBarrierTs updates the barrier ts in this table pipeline
	UpdateBarrierTs(ts model.Ts)
	// AsyncStop tells the pipeline to stop, and returns true is the pipeline is already stopped.
	AsyncStop() bool

	// Start the sink consume data from the given `ts`
	Start(ts model.Ts)

	// Stats returns statistic for a table.
	Stats() Stats
	// State returns the state of this table pipeline
	State() TableState
	// Cancel stops this table pipeline immediately and destroy all resources created by this table pipeline
	Cancel()
	// Wait waits for table pipeline destroyed
	Wait()
	// MemoryConsumption return the memory consumption in bytes
	MemoryConsumption() uint64

	// RemainEvents return the amount of kv events remain in sorter.
	RemainEvents() int64
}

// Key is a custom type for bytes in proto.
type Key []byte

var (
	_ fmt.Stringer = Key{}
	_ fmt.Stringer = (*Key)(nil)
)

func (k Key) String() string {
	return hex.EncodeToString(k)
}

var (
	_ json.Marshaler = Key{}
	_ json.Marshaler = (*Key)(nil)
)

// MarshalJSON implements json.Marshaler.
// It is helpful to format span in log.
func (k Key) MarshalJSON() ([]byte, error) {
	return json.Marshal(k.String())
}

var (
	_ encoding.TextMarshaler = Span{}
	_ encoding.TextMarshaler = (*Span)(nil)
)

// MarshalText implements encoding.TextMarshaler (used in proto.CompactTextString).
// It is helpful to format span in log.
func (s Span) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

func (s *Span) String() string {
	length := len("{table_id:,start_key:,end_key:}")
	length += 8 // for TableID
	length += len(s.StartKey) + len(s.EndKey)
	b := strings.Builder{}
	b.Grow(length)
	b.Write([]byte("{table_id:"))
	b.Write([]byte(strconv.Itoa(int(s.TableID))))
	if len(s.StartKey) > 0 {
		b.Write([]byte(",start_key:"))
		b.Write([]byte(s.StartKey.String()))
	}
	if len(s.EndKey) > 0 {
		b.Write([]byte(",end_key:"))
		b.Write([]byte(s.EndKey.String()))
	}
	b.Write([]byte("}"))
	return b.String()
}

// Less compares two Spans, defines the order between spans.
func (s *Span) Less(b *Span) bool {
	if s.TableID < b.TableID {
		return true
	}
	if bytes.Compare(s.StartKey, b.StartKey) < 0 {
		return true
	}
	return false
}

// Eq compares two Spans, defines the equality between spans.
func (s *Span) Eq(b *Span) bool {
	return s.TableID == b.TableID &&
		bytes.Equal(s.StartKey, b.StartKey) &&
		bytes.Equal(s.EndKey, b.EndKey)
}
