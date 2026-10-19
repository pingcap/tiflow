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
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
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

func TestS3RetryerRetriesReadConnectionReset(t *testing.T) {
	retryer := NewS3Retryer(7, time.Second, 2*time.Second)
	resetErr := newReadConnectionResetError()
	req := newRetryTestRequest(retryer, s3.ServiceName, resetErr)

	// Verify the test error exercises the aws-sdk-go v1 gap fixed here.
	require.False(t, client.DefaultRetryer{NumMaxRetries: 7}.ShouldRetry(req))
	require.True(t, retryer.ShouldRetry(req))
	require.Equal(t, 7, retryer.MaxRetries())

	// ShouldRetry classifies the error, while the SDK separately enforces
	// RetryCount < MaxRetries. Seven means at most seven retries after the
	// initial attempt, not an unbounded loop.
	req.Retryable = aws.Bool(retryer.ShouldRetry(req))
	req.RetryCount = 6
	require.True(t, req.WillRetry())
	req.RetryCount = 7
	require.False(t, req.WillRetry())
}

func TestS3RetryerReadConnectionResetMatcher(t *testing.T) {
	testCases := []struct {
		name  string
		err   error
		match bool
	}{
		{
			name:  "reset by peer",
			err:   errors.New("read tcp 10.0.0.1:1234->10.0.0.2:80: read: connection reset by peer"),
			match: true,
		},
		{
			name:  "reset without peer suffix",
			err:   errors.New("read: connection reset"),
			match: true,
		},
		{
			name:  "write-side reset",
			err:   errors.New("write: connection reset by peer"),
			match: false,
		},
		{
			name:  "generic reset",
			err:   errors.New("connection reset by peer"),
			match: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.match, isConnectionResetError(tc.err))
		})
	}
}

func TestS3RetryerDeadlineTakesPrecedenceOverConnectionReset(t *testing.T) {
	retryer := NewS3Retryer(7, time.Second, 2*time.Second)
	req := newRetryTestRequest(
		retryer,
		s3.ServiceName,
		errors.New("context deadline exceeded while reading: read: connection reset by peer"),
	)

	require.False(t, retryer.ShouldRetry(req))
}

func TestS3RetryerDoesNotPromoteReadConnectionResetForNonS3Service(t *testing.T) {
	retryer := NewS3Retryer(7, time.Second, 2*time.Second)
	req := newRetryTestRequest(retryer, "dynamodb", newReadConnectionResetError())

	require.False(t, client.DefaultRetryer{NumMaxRetries: 7}.ShouldRetry(req))
	require.False(t, retryer.ShouldRetry(req))
}

func TestDefaultS3RetryerRetriesReadConnectionResetWithDefaultBudget(t *testing.T) {
	retryer := DefaultS3Retryer()
	req := newRetryTestRequest(retryer, s3.ServiceName, newReadConnectionResetError())

	require.True(t, retryer.ShouldRetry(req))
	require.Equal(t, 3, retryer.MaxRetries())

	req.Retryable = aws.Bool(retryer.ShouldRetry(req))
	req.RetryCount = 2
	require.True(t, req.WillRetry())
	req.RetryCount = 3
	require.False(t, req.WillRetry())
}

func TestS3RetryerDoesNotRetryDeadline(t *testing.T) {
	retryer := NewS3Retryer(7, time.Second, 2*time.Second)

	deadlineReq := newRetryTestRequest(retryer, s3.ServiceName, errors.New("context deadline exceeded"))
	require.False(t, retryer.ShouldRetry(deadlineReq))
}

func TestS3RetryerDoesNotPromoteEC2MetadataConnectionReset(t *testing.T) {
	retryer := NewS3Retryer(7, time.Second, 2*time.Second)
	metadataReq := newRetryTestRequest(
		retryer, ec2metadata.ServiceName, newReadConnectionResetError())

	require.False(t, retryer.ShouldRetry(metadataReq))
}

func TestS3RetryerKeepsDefaultClassificationForOtherErrors(t *testing.T) {
	retryer := NewS3Retryer(7, time.Second, 2*time.Second)

	accessDeniedReq := newRetryTestRequest(retryer, s3.ServiceName,
		awserr.NewRequestFailure(awserr.New("AccessDenied", "access denied", nil), 403, "request-id"))
	require.False(t, retryer.ShouldRetry(accessDeniedReq))

	defaultRetryableReq := newRetryTestRequest(
		retryer, s3.ServiceName, errors.New("connection reset by peer"))
	require.True(t, retryer.ShouldRetry(defaultRetryableReq))
}

type nonTemporaryError string

func (e nonTemporaryError) Error() string   { return string(e) }
func (e nonTemporaryError) Temporary() bool { return false }

func newReadConnectionResetError() error {
	return awserr.New(
		request.ErrCodeRequestError,
		"send request failed",
		&url.Error{
			Op:  "Put",
			URL: "http://cos.example/bucket/redo.log",
			Err: nonTemporaryError("read tcp 10.0.0.1:1234->10.0.0.2:80: read: connection reset by peer"),
		},
	)
}

func newRetryTestRequest(
	retryer request.Retryer,
	serviceName string,
	err error,
) *request.Request {
	req := &request.Request{
		Retryer: retryer,
		Error:   err,
		Body:    bytes.NewReader(nil),
	}
	req.ClientInfo.ServiceName = serviceName
	return req
}
