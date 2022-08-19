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

package redo

// ManagerOptions defines options for redo log manager.
type ManagerOptions struct {
	// Whether to run background flush goroutine.
	EnableBgRunner bool

	// Whether to start a GC goroutine or not.
	EnableGCRunner bool

	// Whether it's created for emitting redo meta or not.
	EmitMeta bool

	// Whether it's created for emitting row events or not.
	EmitRowEvents bool

	// Whether it's created for emitting DDL events or not.
	EmitDDLEvents bool

	ErrCh chan<- error
}

// NewOwnerManagerOptions creates a manager options for owner.
func NewOwnerManagerOptions(errCh chan<- error) *ManagerOptions {
	return &ManagerOptions{
		EnableBgRunner: true,
		EnableGCRunner: false,
		EmitMeta:       true,
		EmitRowEvents:  false,
		EmitDDLEvents:  true,
		ErrCh:          errCh,
	}
}

// NewProcessorManagerOptions creates a manager options for processor.
func NewProcessorManagerOptions(errCh chan<- error) *ManagerOptions {
	return &ManagerOptions{
		EnableBgRunner: true,
		EnableGCRunner: true,
		EmitMeta:       false,
		EmitRowEvents:  true,
		EmitDDLEvents:  false,
		ErrCh:          errCh,
	}
}

// NewManagerOptionsForClean creates a manager options for cleaning.
func NewManagerOptionsForClean() *ManagerOptions {
	return &ManagerOptions{
		EnableBgRunner: false,
		EnableGCRunner: false,
		EmitMeta:       false,
		EmitRowEvents:  false,
		EmitDDLEvents:  false,
	}
}

// newMockManagerOptions creates a manager options for mock tests.
func newMockManagerOptions(errCh chan<- error) *ManagerOptions {
	return &ManagerOptions{
		EnableBgRunner: true,
		EnableGCRunner: true,
		EmitMeta:       true,
		EmitRowEvents:  true,
		EmitDDLEvents:  true,
		ErrCh:          errCh,
	}
}
