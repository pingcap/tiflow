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

package rpcutil

// ForwardChecker is used for checking whether a request should be forwarded or not.
type ForwardChecker[T any] interface {
	// LeaderOnly returns whether the request is only allowed to handle by the leader.
	LeaderOnly(method string) bool
	// IsLeader returns whether the current node is the leader.
	IsLeader() bool
	// LeaderClient returns the leader client. If there is no leader, it should return
	// nil and errors.ErrMasterNoLeader.
	LeaderClient() (T, error)
}

// FeatureChecker defines an interface that checks whether a feature is available
// or under degradation.
type FeatureChecker interface {
	Available(method string) bool
}
