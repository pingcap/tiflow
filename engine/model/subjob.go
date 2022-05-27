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

package model

type (
	// NodeID defines node id used in DAG
	NodeID int32
	// SubJobID defines sub job id used in DAG
	SubJobID int32
)

// DAG is the directed acyclic graph for SubJobs.
type DAG struct {
	// Root is the root node of the DAG.
	// We represent DAG in a recursive data structure for easier manipulation
	// when building it.
	Root *Node `json:"root"`
}

// Node is a node in the DAG.
type Node struct {
	ID      NodeID  `json:"id"`
	Outputs []*Node `json:"outputs"`

	// TODO more fields to be added in subsequent PRs.
}
