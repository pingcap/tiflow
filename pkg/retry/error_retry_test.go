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
package retry

import (
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestGetRetryBackoff(t *testing.T) {
	t.Parallel()

	r := NewDefaultErrorRetry()
	// test retry backoff
	backoff, err := r.GetRetryBackoff(errors.New("test"))
	require.NoError(t, err)
	require.Less(t, backoff, 30*time.Second)
	time.Sleep(500 * time.Millisecond)
	elapsedTime := time.Since(r.firstRetryTime)

	// mock time to test reset error backoff
	r.lastErrorRetryTime = time.Unix(0, 0)
	_, err = r.GetRetryBackoff(errors.New("test"))
	require.NoError(t, err)
	require.Less(t, time.Since(r.firstRetryTime), elapsedTime)
}
