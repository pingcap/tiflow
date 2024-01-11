// Copyright 2021 PingCAP, Inc.
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
package etcd

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestGetRevisionFromWatchOpts(t *testing.T) {
	t.Parallel()

	for i := 0; i < 100; i++ {
		rev := rand.Int63n(math.MaxInt64)
		opt := clientv3.WithRev(rev)
		require.Equal(t, getRevisionFromWatchOpts(opt), rev)
	}
}
