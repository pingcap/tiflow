// Copyright 2022 PingCAP, Inc.
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

package pebble

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/sorter"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/sorter/pebble/encoding"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestIteratorWithTableFilter(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), t.Name())
	db, err := OpenPebble(
		1, dbPath, &config.DBConfig{Count: 1},
		nil,
		nil,
		// Disable auto compactions to make the case more stable.
		func(opts *pebble.Options) { opts.DisableAutomaticCompactions = true },
	)
	require.Nil(t, err)
	defer func() { _ = db.Close() }()

	writeOpts := &pebble.WriteOptions{Sync: false}

	// Put 7 table keys with CRTS=1, and then flush it to L0. The flush is required for generating table properties.
	for tableID := 1; tableID <= 7; tableID++ {
		key := encoding.EncodeTsKey(1, uint64(tableID), 1, 0)
		b := db.NewBatch()
		b.Set(key, []byte{'x'}, writeOpts)
		if err := b.Commit(writeOpts); err != nil {
			t.Errorf("Put failed: %v", err)
		}
	}
	if err = db.Flush(); err != nil {
		t.Errorf("Flush failed: %v", err)
	}

	// Put 9 table keys with CRTS=3, and then flush it to L0.
	for tableID := 1; tableID <= 9; tableID++ {
		key := encoding.EncodeTsKey(1, uint64(tableID), 3, 0)
		b := db.NewBatch()
		b.Set(key, []byte{'x'}, writeOpts)
		if err := b.Commit(writeOpts); err != nil {
			t.Errorf("Put failed: %v", err)
		}
	}
	if err = db.Flush(); err != nil {
		t.Errorf("Flush failed: %v", err)
	}

	// Sleep a while. Automatic compactions shouldn't be triggered.
	time.Sleep(time.Second)

	// There should be no any compactions but 2 tables at L0.
	stats := db.Metrics()
	require.Equal(t, int64(0), stats.Compact.Count)
	require.Equal(t, int64(2), stats.Levels[0].NumFiles)
	// 7 is a pebble internal constant.
	// See: https://github.com/cockroachdb/pebble/blob/
	// 71d17c2a007bfad5111a229ba325d30251b88a41/internal/manifest/version.go#L579
	for level := 1; level < 7; level++ {
		require.Equal(t, int64(0), stats.Levels[level].NumFiles)
	}

	for _, x := range []struct {
		lowerTs, upperTs uint64
		expectedCount    int
	}{
		{lowerTs: 0, upperTs: 1, expectedCount: 7},
		{lowerTs: 1, upperTs: 2, expectedCount: 7},
		{lowerTs: 2, upperTs: 3, expectedCount: 9},
		{lowerTs: 3, upperTs: 4, expectedCount: 9},
		{lowerTs: 0, upperTs: 10, expectedCount: 16},
		{lowerTs: 10, upperTs: 20, expectedCount: 0},
	} {
		count := 0
		for tableID := 0; tableID <= 9; tableID++ {
			iter := iterTable(db, 1, model.TableID(tableID),
				sorter.Position{CommitTs: x.lowerTs},
				sorter.Position{CommitTs: x.upperTs}.Next())
			valid := iter.Valid()
			for valid {
				count += 1
				valid = iter.Next()
			}
			require.Nil(t, iter.Close())
		}
		require.Equal(t, x.expectedCount, count)
	}
}
