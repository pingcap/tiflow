package model

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/pkg/adapter"
)

func TestTerminateState(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		code     WorkerStatusCode
		expected bool
	}{
		{WorkerStatusNormal, false},
		{WorkerStatusCreated, false},
		{WorkerStatusInit, false},
		{WorkerStatusError, true},
		{WorkerStatusFinished, true},
		{WorkerStatusStopped, true},
	}
	s := &WorkerStatus{}
	for _, tc := range testCases {
		s.Code = tc.code
		require.Equal(t, tc.expected, s.InTerminateState())
	}
}

func TestHasSignificantChange(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		prev, next *WorkerStatus
		changed    bool
	}{
		{
			prev:    &WorkerStatus{Code: WorkerStatusInit, ErrorMessage: "1"},
			next:    &WorkerStatus{Code: WorkerStatusInit, ErrorMessage: "2"},
			changed: true,
		},
		{
			prev:    &WorkerStatus{Code: WorkerStatusFinished, ErrorMessage: "1"},
			next:    &WorkerStatus{Code: WorkerStatusFinished, ErrorMessage: "1"},
			changed: false,
		},
		{
			prev:    &WorkerStatus{Code: WorkerStatusFinished, ErrorMessage: "1", ExtBytes: []byte("1")},
			next:    &WorkerStatus{Code: WorkerStatusFinished, ErrorMessage: "1", ExtBytes: []byte("2")},
			changed: false,
		},
		{
			prev:    &WorkerStatus{Code: WorkerStatusFinished, ErrorMessage: "1", ExtBytes: []byte("1")},
			next:    &WorkerStatus{Code: WorkerStatusFinished, ErrorMessage: "2", ExtBytes: []byte("2")},
			changed: true,
		},
		{
			prev:    &WorkerStatus{Code: WorkerStatusInit, ErrorMessage: "1"},
			next:    &WorkerStatus{Code: WorkerStatusNormal, ErrorMessage: "1"},
			changed: true,
		},
	}

	for _, tc := range testCases {
		changed := tc.prev.HasSignificantChange(tc.next)
		require.Equal(t, tc.changed, changed)
	}
}

func TestEncodeWorkerStatusKey(t *testing.T) {
	require.Equal(
		t,
		adapter.WorkerKeyAdapter.Encode("a", "b"),
		EncodeWorkerStatusKey("a", "b"))
}
