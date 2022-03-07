// Copyright 2021 PingCAP, Inc.
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

package message

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sorter/encoding"
	"github.com/stretchr/testify/require"
)

func TestPrint(t *testing.T) {
	t.Parallel()
	event := model.NewPolymorphicEvent(&model.RawKVEntry{
		OpType:  model.OpTypeDelete,
		Key:     []byte{1},
		StartTs: 3,
		CRTs:    4,
	})

	require.Equal(t, "uid: 1, tableID: 2, startTs: 3, CRTs: 4",
		Key(encoding.EncodeKey(1, 2, event)).String())
	require.Equal(t, "uid: 1, tableID: 2, startTs: 0, CRTs: 3",
		Key(encoding.EncodeTsKey(1, 2, 3)).String())
}
