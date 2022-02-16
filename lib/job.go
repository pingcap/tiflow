package lib

import (
	"encoding/json"
)

// MasterMetaExt holds the config of a job and used for failover
type MasterMetaExt struct {
	ID MasterID   `json:"id"`
	Tp WorkerType `json:"type"`

	// config stores raw config from submit-job, the raw config can be
	// reflected to real type via DeserializeConfig
	Config     []byte `json:"config"`
	Checkpoint []byte `json:"checkpoint"`
}

func (meta *MasterMetaExt) Marshal() ([]byte, error) {
	return json.Marshal(meta)
}

func (meta *MasterMetaExt) Unmarshal(data []byte) error {
	return json.Unmarshal(data, meta)
}
