// Copyright 2022 PingCAP, Inc.
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

package dataset

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tiflow/engine/pkg/adapter"
	"github.com/pingcap/tiflow/engine/pkg/meta/mock"
	"github.com/stretchr/testify/require"
)

type record struct {
	RID   string
	Value int
}

func (r *record) GetID() string {
	return r.RID
}

func TestDataSetBasics(t *testing.T) {
	mockKV := mock.NewMetaMock()
	dataset := NewDataSet[record, *record](mockKV, adapter.MasterInfoKey)
	err := dataset.Upsert(context.TODO(), &record{
		RID:   "123",
		Value: 123,
	})
	require.NoError(t, err)

	err = dataset.Upsert(context.TODO(), &record{
		RID:   "123",
		Value: 456,
	})
	require.NoError(t, err)

	rec, err := dataset.Get(context.TODO(), "123")
	require.NoError(t, err)
	require.Equal(t, &record{
		RID:   "123",
		Value: 456,
	}, rec)

	err = dataset.Delete(context.TODO(), "123")
	require.NoError(t, err)

	_, err = dataset.Get(context.TODO(), "123")
	require.Error(t, err)
	require.Regexp(t, ".*ErrDatasetEntryNotFound.*", err.Error())
}

func TestDataSetLoadAll(t *testing.T) {
	mockKV := mock.NewMetaMock()
	dataset := NewDataSet[record, *record](mockKV, adapter.MasterInfoKey)
	for i := 0; i < 1024; i++ {
		err := dataset.Upsert(context.TODO(), &record{
			RID:   fmt.Sprintf("%d", i),
			Value: i,
		})
		require.NoError(t, err)
	}

	recs, err := dataset.LoadAll(context.TODO())
	require.NoError(t, err)
	require.Len(t, recs, 1024)
}
