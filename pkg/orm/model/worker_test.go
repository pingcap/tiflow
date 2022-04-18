package model

import (
	"testing"

	"github.com/stretchr/testify/require"
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
	s := &WorkerMeta{}
	for _, tc := range testCases {
		s.Status = tc.code
		require.Equal(t, tc.expected, s.InTerminateState())
	}
}

func TestHasSignificantChange(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		prev, next *WorkerMeta
		changed    bool
	}{
		{
			prev:    &WorkerMeta{Status: WorkerStatusInit, ErrorMessage: "1"},
			next:    &WorkerMeta{Status: WorkerStatusInit, ErrorMessage: "2"},
			changed: true,
		},
		{
			prev:    &WorkerMeta{Status: WorkerStatusFinished, ErrorMessage: "1"},
			next:    &WorkerMeta{Status: WorkerStatusFinished, ErrorMessage: "1"},
			changed: false,
		},
		{
			prev:    &WorkerMeta{Status: WorkerStatusFinished, ErrorMessage: "1", ExtBytes: []byte("1")},
			next:    &WorkerMeta{Status: WorkerStatusFinished, ErrorMessage: "1", ExtBytes: []byte("2")},
			changed: false,
		},
		{
			prev:    &WorkerMeta{Status: WorkerStatusFinished, ErrorMessage: "1", ExtBytes: []byte("1")},
			next:    &WorkerMeta{Status: WorkerStatusFinished, ErrorMessage: "2", ExtBytes: []byte("2")},
			changed: true,
		},
		{
			prev:    &WorkerMeta{Status: WorkerStatusInit, ErrorMessage: "1"},
			next:    &WorkerMeta{Status: WorkerStatusNormal, ErrorMessage: "1"},
			changed: true,
		},
	}

	for _, tc := range testCases {
		changed := tc.prev.HasSignificantChange(tc.next)
		require.Equal(t, tc.changed, changed)
	}
}
