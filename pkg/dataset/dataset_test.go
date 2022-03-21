package dataset

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/meta/kvclient/mock"
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
