package outdated

import (
	"encoding/json"

	"github.com/pingcap/tidb-tools/pkg/filter"
)

// ReplicaConfig represents some addition replication config for a changefeed
type ReplicaConfigV1 struct {
	Sink *struct {
		DispatchRules []*struct {
			filter.Table
			Rule string `toml:"rule" json:"rule"`
		} `toml:"dispatch-rules" json:"dispatch-rules"`
	} `toml:"sink" json:"sink"`
}

// Unmarshal unmarshals into *ReplicaConfigV1 from json marshal byte slice
func (c *ReplicaConfigV1) Unmarshal(data []byte) error {
	return json.Unmarshal(data, c)
}
