package util

import "github.com/pingcap/tidb-tools/pkg/filter"

// Some cyclic replication implemention are here, this is required to break
// cyclic imports.
//
// package cyclic imports model
//         model imports util
// so util can not imports cyclic
//
// TODO(neil) move it package cyclic

const (
	// CyclicSchemaName is the name of schema where all mark tables are created
	CyclicSchemaName string = "tidb_cdc"
)

// CyclicConfig represents config used for cyclic replication
type CyclicConfig struct {
	ReplicaID       uint64   `toml:"enable" json:"enable"`
	SyncDDL         bool     `toml:"sync-ddl" json:"sync-ddl"`
	IDBuckets       int      `toml:"id-buckets" json:"id-buckets"`
	FilterReplicaID []uint64 `toml:"filter-replica-ids" json:"filter-replica-ids"`
}

func newCyclicFitler(config *CyclicConfig) (*filter.Filter, error) {
	if config.ReplicaID == 0 {
		return nil, nil
	}
	caseSensitive := true
	if config.SyncDDL {
		// Filter DDLs targets to cyclic.SchemaName
		rules := &filter.Rules{
			IgnoreDBs: []string{CyclicSchemaName},
		}
		cyclicFilter, err := filter.New(caseSensitive, rules)
		if err != nil {
			return nil, err
		}
		return cyclicFilter, nil
	}

	// Filter all DDLs
	rules := &filter.Rules{
		IgnoreDBs: []string{"~.*"},
	}
	cyclicFilter, err := filter.New(!caseSensitive, rules)
	if err != nil {
		return nil, err
	}
	return cyclicFilter, nil
}
