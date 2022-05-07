package model

type (
	NodeID   int32
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
