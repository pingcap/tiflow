// Copyright 2025 PingCAP, Inc.
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

package util

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
)

// mockRoundTripper blocks until the context is done.
type mockRoundTripper struct {
	blockUntilContextDone bool
	err                   error
}

func (m *mockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if m.blockUntilContextDone {
		<-req.Context().Done()
		return nil, req.Context().Err()
	}
	// Return immediately for success case
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, m.err
}

// mockExternalStorage is a mock implementation for testing timeouts via http client.
type mockExternalStorage struct {
	storage.ExternalStorage // Embed the interface to satisfy it easily
	httpClient              *http.Client
}

// WriteFile simulates a write operation by making an HTTP request that respects context cancellation.
func (m *mockExternalStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	if m.httpClient == nil {
		panic("httpClient not set in mockExternalStorage") // Should be set in tests
	}
	// Create a dummy request. The URL doesn't matter as the RoundTripper is mocked.
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://mock/"+name, http.NoBody)
	if err != nil {
		return err // Should not happen with valid inputs
	}

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return err // This will include context errors like DeadlineExceeded
	}
	resp.Body.Close() // Important to close the body
	if resp.StatusCode != http.StatusOK {
		return errors.New("mock http request failed") // Or handle specific statuses
	}
	return nil
}

// Implement Open so tests can simulate readers that bind to the Open() context.
func (m *mockExternalStorage) Open(ctx context.Context, path string, option *storage.ReaderOption) (storage.ExternalFileReader, error) {
	return &ctxBoundReader{ctx: ctx}, nil
}

func (m *mockExternalStorage) URI() string { return "mock://" }

func (m *mockExternalStorage) Close() {}

func TestExtStorageWithTimeoutWriteFileTimeout(t *testing.T) {
	testTimeout := 50 * time.Millisecond

	// Create a mock HTTP client that blocks until context is done
	mockClient := &http.Client{
		Transport: &mockRoundTripper{blockUntilContextDone: true},
	}

	mockStore := &mockExternalStorage{
		httpClient: mockClient,
	}

	// Wrap the mock store with the timeout logic
	timedStore := &extStorageWithTimeout{
		ExternalStorage: mockStore,
		timeout:         testTimeout,
	}

	startTime := time.Now()
	// Use context.Background() as the base context
	err := timedStore.WriteFile(context.Background(), "testfile", []byte("data"))
	duration := time.Since(startTime)

	// 1. Assert that an error occurred
	require.Error(t, err, "Expected an error due to timeout")

	// 2. Assert that the error is context.DeadlineExceeded
	require.True(t, errors.Is(err, context.DeadlineExceeded), "Expected context.DeadlineExceeded error, got: %v", err)

	// 3. Assert that the function returned quickly (around the timeout duration)
	require.InDelta(t, testTimeout, duration, float64(testTimeout)*0.5, "Duration (%v) should be close to the timeout (%v)", duration, testTimeout)
}

func TestExtStorageWithTimeoutWriteFileSuccess(t *testing.T) {
	testTimeout := 100 * time.Millisecond

	// Create a mock HTTP client that returns success immediately
	mockClient := &http.Client{
		Transport: &mockRoundTripper{blockUntilContextDone: false, err: nil},
	}

	mockStore := &mockExternalStorage{
		httpClient: mockClient,
	}

	timedStore := &extStorageWithTimeout{
		ExternalStorage: mockStore,
		timeout:         testTimeout,
	}

	// Use context.Background() as the base context
	err := timedStore.WriteFile(context.Background(), "testfile", []byte("data"))

	// Assert success
	require.NoError(t, err, "Expected no error for successful write within timeout")
}

// ctxBoundReader is a reader that checks the context passed to Open().
// It simulates backends (e.g., Azure) that bind reader lifetime to the Open() context.
type ctxBoundReader struct {
	ctx context.Context
}

func (r *ctxBoundReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	p[0] = 'x'
	return 1, nil
}

func (r *ctxBoundReader) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

func (r *ctxBoundReader) Close() error { return nil }

func (r *ctxBoundReader) GetFileSize() (int64, error) { return 1, nil }

func TestExtStorageOpenDoesNotCancelReaderContext(t *testing.T) {
	timedStore := &extStorageWithTimeout{
		ExternalStorage: &mockExternalStorage{},
		timeout:         100 * time.Millisecond,
	}

	rd, err := timedStore.Open(context.Background(), "file", nil)
	require.NoError(t, err)
	defer rd.Close()

	// If Open() had used a derived context with immediate cancel, this would fail with context canceled.
	_, err = rd.Read(make([]byte, 1))
	require.NoError(t, err)
}

func TestExtStorageOpenReaderRespectsCallerCancel(t *testing.T) {
	timedStore := &extStorageWithTimeout{
		ExternalStorage: &mockExternalStorage{},
		timeout:         10 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	rd, err := timedStore.Open(ctx, "file", nil)
	require.NoError(t, err)
	defer rd.Close()

	// This should cause the reader to fail with context canceled.
	cancel()
	_, err = rd.Read(make([]byte, 1))

	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded))
}
