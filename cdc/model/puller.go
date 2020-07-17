package model

// SorterStatus is the state of the puller sorter
type SorterStatus = int32

// SorterStatus of the puller sorter
const (
	SorterStatusWorking SorterStatus = iota
	SorterStatusStopping
	SorterStatusStopped
)
