package model

type (
	// MasterID is master id in master worker framework.
	// - It is job manager id when master is job manager and worker is job master.
	// - It is job master id when master is job master and worker is worker.
	MasterID = string
	// WorkerID is worker id in master worker framework.
	// - It is job master id when master is job manager and worker is job master.
	// - It is worker id when master is job master and worker is worker.
	WorkerID = string
	// Epoch is an increasing only value
	Epoch = int64
	// JobType is the unique identifier for job
	JobType = string
)
