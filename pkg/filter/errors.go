// Copyright 2020 PingCAP, Inc.
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

package filter

<<<<<<< HEAD:pkg/filter/errors.go
import (
	"github.com/pingcap/errors"
	cerror "github.com/pingcap/ticdc/pkg/errors"
)

// ChangefeedFastFailError checks the error, returns true if it is meaningless
// to retry on this error
func ChangefeedFastFailError(err error) bool {
	return cerror.ErrStartTsBeforeGC.Equal(errors.Cause(err))
=======
// KVClientConfig represents config for kv client
type KVClientConfig struct {
	// how many workers will be used for a single region worker
	WorkerConcurrent int `toml:"worker-concurrent" json:"worker-concurrent"`
	// background workerpool size, the workrpool is shared by all goroutines in cdc server
	WorkerPoolSize int `toml:"worker-pool-size" json:"worker-pool-size"`
	// region incremental scan limit for one table in a single store
	RegionScanLimit int `toml:"region-scan-limit" json:"region-scan-limit"`
>>>>>>> f7ab5ba4 (kv/client: add incremental scan region count limit (#1899)):pkg/config/kvclient.go
}
