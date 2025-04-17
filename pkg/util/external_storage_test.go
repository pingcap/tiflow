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
	"os"
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

// TestExtStorageWithTimeoutWriteFileAzureTimeout - Tests WriteFile timeout for Azure.
// NOTE: This test requires Azure Blob Storage connection details (e.g., via environment variables)
// and a valid Azure storage URI (e.g., "azure://<container-name>/<path>?account-key=...")
// Or configure it to use Azurite emulator.
//
// You can set the environment variable TiCDC_TEST_AZURE_URI to the Azure storage URI for testing.
// For example:
// export TiCDC_TEST_AZURE_URI="azure://testcontainer/timeouttest?account-name=$AZURE_STORAGE_ACCOUNT&account-key=$AZURE_STORAGE_KEY"
//
// If you want to use Azurite emulator, you can set the environment variable TiCDC_TEST_AZURE_URI to:
// export TiCDC_TEST_AZURE_URI="http://127.0.0.1:10000/devstoreaccount1/timeouttest"

func TestExtStorageWithTimeoutWriteFileAzureTimeout(t *testing.T) {
	// 1. Get Azure URI from environment or skip test
	azureURI := os.Getenv("TiCDC_TEST_AZURE_URI") // Example: "azure://testcontainer/timeouttest"
	if azureURI == "" {
		t.Skip("TiCDC_TEST_AZURE_URI environment variable is not set, skipping Azure timeout test")
	}

	ctx := context.Background()

	// 2. Create base Azure external storage (without timeout wrapper first)
	// Ensure necessary Azure credentials (like AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY or AZURE_STORAGE_CONNECTION_STRING)
	// are available in the environment for storage.New to work.
	baseStorage, err := GetExternalStorage(ctx, azureURI, nil, DefaultS3Retryer()) // Using DefaultS3Retryer might not be ideal for Azure, but fine for this test structure
	require.NoError(t, err, "Failed to create base Azure external storage")

	// 3. Create the wrapper with a very short timeout
	veryShortTimeout := 1 * time.Millisecond // Choose a timeout likely to be exceeded
	storageWithTimeout := &extStorageWithTimeout{
		ExternalStorage: baseStorage,
		timeout:         veryShortTimeout,
	}

	// 4. Attempt to write a file
	testFileName := "timeout_test_file.txt"
	testData := []byte("hello azure timeout")

	startTime := time.Now()
	err = storageWithTimeout.WriteFile(ctx, testFileName, testData)
	duration := time.Since(startTime)

	// 5. Assertions
	require.Error(t, err, "WriteFile should have returned an error due to timeout")
	require.True(t, errors.Is(err, context.DeadlineExceeded), "Error should be context.DeadlineExceeded, but got: %v", err)
	require.Less(t, duration, veryShortTimeout+500*time.Millisecond, "Operation took much longer than expected after timeout") // Check it didn't wait indefinitely

	// Optional: Cleanup the file if it somehow got created (unlikely with timeout)
	// It's better practice to use a unique path/container per test run and clean the container afterwards.
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // Use a reasonable timeout for cleanup
	defer cancel()
	baseStorage.DeleteFile(cleanupCtx, testFileName) // Use baseStorage for cleanup
}
