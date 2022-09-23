// Copyright 2022 PingCAP, Inc.
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
package fsutil

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileAllocateSuccess(t *testing.T) {
	fl := NewFileAllocator(t.TempDir(), "test", 64*1024*1024)
	defer fl.Close()

	f, err := fl.Open()
	require.Nil(t, err)
	f.Close()
}

func TestFileAllocateFailed(t *testing.T) {
	// 1. the requested allocation space will cause disk full
	fl := NewFileAllocator(t.TempDir(), "test", math.MaxInt64)

	f, err := fl.Open()
	require.Nil(t, f)
	require.NotNil(t, err)
	f.Close()
	fl.Close()

	// 2. the directory does not exist
	fl = NewFileAllocator("not-exist-dir", "test", 1024)
	f, err = fl.Open()
	require.NotNil(t, err)
	require.Nil(t, f)
	fl.Close()
}

func benchmarkWriteData(b *testing.B, size int, useFileAlloctor bool) {
	var f *os.File
	var err error

	if useFileAlloctor {
		fl := NewFileAllocator(b.TempDir(), "bench", 64*1024*1024)
		defer fl.Close()

		f, err := fl.Open()
		require.NotNil(b, f)
		require.Nil(b, err)
	} else {
		f, err = os.OpenFile(filepath.Join(b.TempDir(), "bench"),
			os.O_CREATE|os.O_WRONLY, 0o420)
		require.Nil(b, err)
	}

	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = byte(i)
	}

	b.ResetTimer()
	b.SetBytes(int64(8 * size))
	for i := 0; i < b.N; i++ {
		_, err := f.Write(data)
		f.Sync()
		require.Nil(b, err)
	}
}

// go test -bench ^BenchmarkWrite100EntryWithAllocator$ \
// -benchtime=30000x github.com/pingcap/tiflow/pkg/fsutil
// goos: linux
// goarch: amd64
// pkg: github.com/pingcap/tiflow/pkg/fsutil
// cpu: Intel(R) Xeon(R) Gold 6240 CPU @ 2.60GHz
// BenchmarkWrite100EntryWithAllocator-9   	   30000	    204681 ns/op	   3.91 MB/s
func BenchmarkWrite100EntryWithAllocator(b *testing.B) {
	benchmarkWriteData(b, 100, true)
}

// go test -bench ^BenchmarkWrite100EntryWithoutAllocator$ \
// -benchtime=30000x github.com/pingcap/tiflow/pkg/fsutil
// goos: linux
// goarch: amd64
// pkg: github.com/pingcap/tiflow/pkg/fsutil
// cpu: Intel(R) Xeon(R) Gold 6240 CPU @ 2.60GHz
// BenchmarkWrite100EntryWithoutAllocator-9   	   30000	    303590 ns/op	   2.64 MB/s
func BenchmarkWrite100EntryWithoutAllocator(b *testing.B) {
	benchmarkWriteData(b, 100, false)
}

// go test -bench ^BenchmarkWrite1000EntryWithAllocator$ \
// -benchtime=30000x github.com/pingcap/tiflow/pkg/fsutil
// goos: linux
// goarch: amd64
// pkg: github.com/pingcap/tiflow/pkg/fsutil
// cpu: Intel(R) Xeon(R) Gold 6240 CPU @ 2.60GHz
// BenchmarkWrite1000EntryWithAllocator-9   	   30000	    216718 ns/op	  36.91 MB/s
func BenchmarkWrite1000EntryWithAllocator(b *testing.B) {
	benchmarkWriteData(b, 1000, true)
}

// go test -bench ^BenchmarkWrite1000EntryWithoutAllocator$ \
// -benchtime=30000x github.com/pingcap/tiflow/pkg/fsutil
// goos: linux
// goarch: amd64
// pkg: github.com/pingcap/tiflow/pkg/fsutil
// cpu: Intel(R) Xeon(R) Gold 6240 CPU @ 2.60GHz
// BenchmarkWrite1000EntryWithoutAllocator-9   	   30000	    293505 ns/op	  27.26 MB/s
func BenchmarkWrite1000EntryWithoutAllocator(b *testing.B) {
	benchmarkWriteData(b, 1000, false)
}

// go test -bench ^BenchmarkWrite10000EntryWithAllocator$ \
// -benchtime=30000x github.com/pingcap/tiflow/pkg/fsutil
// goos: linux
// goarch: amd64
// pkg: github.com/pingcap/tiflow/pkg/fsutil
// cpu: Intel(R) Xeon(R) Gold 6240 CPU @ 2.60GHz
// BenchmarkWrite10000EntryWithAllocator-9   	   30000	    333734 ns/op	 239.71 MB/s
func BenchmarkWrite10000EntryWithAllocator(b *testing.B) {
	benchmarkWriteData(b, 10000, true)
}

// go test -bench ^BenchmarkWrite10000EntryWithoutAllocator$ \
// -benchtime=30000x github.com/pingcap/tiflow/pkg/fsutil
// goos: linux
// goarch: amd64
// pkg: github.com/pingcap/tiflow/pkg/fsutil
// cpu: Intel(R) Xeon(R) Gold 6240 CPU @ 2.60GHz
// BenchmarkWrite10000EntryWithoutAllocator-9   	   30000	    399723 ns/op	 200.14 MB/s
func BenchmarkWrite10000EntryWithoutAllocator(b *testing.B) {
	benchmarkWriteData(b, 10000, false)
}
