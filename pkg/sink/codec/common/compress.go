// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/compression"
	"github.com/pingcap/tiflow/pkg/errors"
)

// Compress the given data by the given compression, also record the compression ratio metric.
func Compress(changefeedID model.ChangeFeedID, cc string, data []byte) ([]byte, error) {
	oldSize := len(data)
	compressed, err := compression.Encode(cc, data)
	if err != nil {
		return nil, errors.Trace(err)
	}

	newSize := len(compressed)
	ratio := float64(oldSize) / float64(newSize) * 100

	compressionRatio.WithLabelValues(changefeedID.Namespace, changefeedID.ID).Observe(ratio)

	return compressed, nil
}

// Decompress the given data by the given compression.
func Decompress(cc string, data []byte) ([]byte, error) {
	return compression.Decode(cc, data)
}
