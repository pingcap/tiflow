// Copyright 2021 PingCAP, Inc.
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

package unified

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestNoLeaking(t *testing.T) {
	bknd := newMemoryBackEnd()
	wrtr, err := bknd.writer()
	require.Nil(t, err)

	var objCount int64
	for i := 0; i < 10000; i++ {
		atomic.AddInt64(&objCount, 1)
		event := model.NewResolvedPolymorphicEvent(0, 1)
		runtime.SetFinalizer(event, func(*model.PolymorphicEvent) {
			atomic.AddInt64(&objCount, -1)
		})
		err := wrtr.writeNext(event)
		require.Nil(t, err)
	}
	err = wrtr.flushAndClose()
	require.Nil(t, err)

	rdr, err := bknd.reader()
	require.Nil(t, err)

	for i := 0; i < 5000; i++ {
		_, err := rdr.readNext()
		require.Nil(t, err)
	}

	for i := 0; i < 10; i++ {
		runtime.GC()
		if atomic.LoadInt64(&objCount) <= 5000 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.LessOrEqual(t, atomic.LoadInt64(&objCount), int64(5000))

	err = rdr.resetAndClose()
	require.Nil(t, err)

	for i := 0; i < 10; i++ {
		runtime.GC()
		if atomic.LoadInt64(&objCount) == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Equal(t, int64(0), atomic.LoadInt64(&objCount))
}
