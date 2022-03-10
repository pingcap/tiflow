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
	"strings"
	"sync"
)

// UniqueKey is the data structure describing a unique key.
type UniqueKey struct {
	// It inherits a RWMutex, which is used to modify the metadata inside the UK struct.
	sync.RWMutex
	// OPLock is the lock for operating on the unique key
	OPLock sync.Mutex
	// RowID is an integer describing the row ID of the unique key.
	// The row ID is a virtual concept, not the real row ID for a DB table.
	// Usually it is used to locate the index in an MCP.
	RowID int
	// Value is the real value of all the UK columns.
	// The key is the column name, the value is the real value.
	Value map[string]interface{}
}

// Clone is to clone a UK into a new one.
// So that two UK objects are not interfered with each other.
func (uk *UniqueKey) Clone() *UniqueKey {
	result := &UniqueKey{
		RowID: uk.RowID,
		Value: map[string]interface{}{},
	}
	uk.RLock()
	defer uk.RUnlock()
	for k, v := range uk.Value {
		result.Value[k] = v
	}
	return result
}

// String returns the string representation of a UK.
func (uk *UniqueKey) String() string {
	uk.RLock()
	defer uk.RUnlock()
	var b strings.Builder
	fmt.Fprintf(&b, "%p: { RowID: %d, ", uk, uk.RowID)
	fmt.Fprintf(&b, "Keys: ( ")
	for k, v := range uk.Value {
		fmt.Fprintf(&b, "%s = %v; ", k, v)
	}
	fmt.Fprintf(&b, ") }")
	return b.String()
}

// IsValueEqual tests whether two UK's value parts are equal.
func (uk *UniqueKey) IsValueEqual(otherUK *UniqueKey) bool {
	uk.RLock()
	defer uk.RUnlock()
	if otherUK == nil {
		return false
	}
	if len(uk.Value) != len(otherUK.Value) {
		return false
	}
	for k, v := range uk.Value {
		otherV, ok := otherUK.Value[k]
		if !ok {
			return false
		}
		if v != otherV {
			return false
		}
	}
	return true
}
