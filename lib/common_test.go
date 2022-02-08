package lib

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type dummyExtField struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func TestWorkerStatusFillExt(t *testing.T) {
	t.Parallel()

	ws := &WorkerStatus{
		ExtBytes: []byte(`{"id":10,"name":"test"}`),
	}
	err := ws.fillExt(&dummyExtField{})
	require.Nil(t, err)
	require.Equal(t, &dummyExtField{ID: 10, Name: "test"}, ws.Ext)

	ws = &WorkerStatus{
		ExtBytes: []byte("10"),
	}
	emptyInt := int64(0)
	err = ws.fillExt(&emptyInt)
	require.Nil(t, err)
	require.Equal(t, int64(10), *ws.Ext.(*int64))

	ws = &WorkerStatus{
		ExtBytes: []byte("10"),
	}
	err = ws.fillExt(int64(0))
	require.Regexp(t, ".*reflect: Elem of invalid type int64", err)
}
