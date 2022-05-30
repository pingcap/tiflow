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
	"github.com/pingcap/errors"
)

var (
	// ErrMCPCapacityFull means the capacity of the modification candidate pool (MCP) is full.
	ErrMCPCapacityFull = errors.New("the capacity of the modification candidate pool is full")
	// ErrInvalidRowID means the row ID of the unique key is invalid.
	// For example, when the row ID is greater than the current MCP size, this error will be triggered.
	ErrInvalidRowID = errors.New("invalid row ID")
	// ErrDeleteUKNotFound means the unique key to be deleted is not found in the MCP.
	ErrDeleteUKNotFound = errors.New("delete UK not found")
)
