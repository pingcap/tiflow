// Copyright 2022 PingCAP, Inc.
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

package mcp

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// UniqueKey is the data structure describing a unique key.
type UniqueKey struct {
	// It inherits a RWMutex, which is used to modify the metadata inside the UK struct.
	sync.RWMutex
	// rowID is an integer describing the row ID of the unique key.
	// The row ID is a virtual concept, not the real row ID for a DB table.
	// Usually it is used to locate the index in an MCP.
	rowID int
	// value is the real value of all the UK columns.
	// The key is the column name, the value is the real value.
	value map[string]interface{}
}

// NewUniqueKey creates a new unique key instance.
// the map values are cloned into the new UK instance,
// so that the further changes in the value map won't affect the values inside the UK.
func NewUniqueKey(rowID int, value map[string]interface{}) *UniqueKey {
	result := &UniqueKey{
		rowID: rowID,
		value: make(map[string]interface{}),
	}
	for k, v := range value {
		result.value[k] = v
	}
	return result
}

// GetRowID gets the row ID of the unique key.
// The row ID is a virtual concept, not the real row ID for a DB table.
// Usually it is used to locate the index in an MCP.
func (uk *UniqueKey) GetRowID() int {
	uk.RLock()
	defer uk.RUnlock()
	return uk.rowID
}

// SetRowID sets the row ID of the unique key.
func (uk *UniqueKey) SetRowID(rowID int) {
	uk.Lock()
	defer uk.Unlock()
	uk.rowID = rowID
}

// GetValue gets the UK value map of a unique key.
// The returned value is cloned, so that further modifications won't affect the value inside the UK.
func (uk *UniqueKey) GetValue() map[string]interface{} {
	uk.RLock()
	defer uk.RUnlock()
	result := make(map[string]interface{})
	for k, v := range uk.value {
		result[k] = v
	}
	return result
}

// GetValueHash return hash for values.
func (uk *UniqueKey) GetValueHash() string {
	uk.RLock()
	defer uk.RUnlock()

	keys := make([]string, 0)
	for k := range uk.value {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		v := uk.value[k]
		fmt.Fprintf(&b, "%s = %v; ", k, v)
	}
	return b.String()
}

// SetValue sets the UK value map.
// The input values are cloned into the UK,
// and further modifications on the input map won't affect the values inside the UK.
func (uk *UniqueKey) SetValue(value map[string]interface{}) {
	uk.Lock()
	defer uk.Unlock()
	uk.value = make(map[string]interface{})
	for k, v := range value {
		uk.value[k] = v
	}
}

// Clone is to clone a UK into a new one.
// So that two UK objects are not interfered with each other.
func (uk *UniqueKey) Clone() *UniqueKey {
	uk.RLock()
	defer uk.RUnlock()
	result := &UniqueKey{
		rowID: uk.rowID,
		value: map[string]interface{}{},
	}
	for k, v := range uk.value {
		result.value[k] = v
	}
	return result
}

// String returns the string representation of a UK.
func (uk *UniqueKey) String() string {
	uk.RLock()
	defer uk.RUnlock()
	var b strings.Builder
	fmt.Fprintf(&b, "%p: { RowID: %d, ", uk, uk.rowID)
	fmt.Fprintf(&b, "Keys: ( ")
	for k, v := range uk.value {
		fmt.Fprintf(&b, "%s = %v; ", k, v)
	}
	fmt.Fprintf(&b, ") }")
	return b.String()
}

// IsValueEqual tests whether two UK's value parts are equal.
func (uk *UniqueKey) IsValueEqual(otherUK *UniqueKey) bool {
	if uk == nil || otherUK == nil {
		return false
	}
	uk.RLock()
	defer uk.RUnlock()
	otherUK.RLock()
	defer otherUK.RUnlock()
	if len(uk.value) != len(otherUK.value) {
		return false
	}
	for k, v := range uk.value {
		otherV, ok := otherUK.value[k]
		if !ok {
			return false
		}
		if v != otherV {
			return false
		}
	}
	return true
}
