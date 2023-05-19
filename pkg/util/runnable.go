// Copyright 2023 PingCAP, Inc.
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

package util

import (
	"context"
)

// Runnable is for handling sub components in a unified way.
//
// For example:
// r := createRunable()
// ctx, cancel := context.WithCancel(context.Background())
// go func() { handleError(r.Run(ctx)) }()
// r.WaitForReady()
// cancel()
// r.Close()
type Runnable interface {
	// Run all sub goroutines and block the current one. If an error occurs
	// in any sub goroutine, return it and cancel all others.
	//
	// Generally a Runnable object can have some internal resources, like file
	// descriptors, channels or memory buffers. Those resources may be still
	// necessary by other components after this Runnable is returned. We can use
	// Close to release them.
	//
	// `warnings` is used to retrieve internal warnings generated when running.
	Run(ctx context.Context, warnings ...chan<- error) error
	// WaitForReady blocks the current goroutine until `Run` initializes itself.
	WaitForReady(ctx context.Context)
	// Close all internal resources synchronously.
	Close()
}
