package lib

// JobMasterV2 holds the config of a job and used for failover
type JobMasterV2 struct {
	ID         MasterID    `json:"id"`
	Tp         WorkerType  `json:"type"`
	Ext        interface{} `json:"ext"`
	Checkpoint []byte      `json:"checkpoint"`
}
