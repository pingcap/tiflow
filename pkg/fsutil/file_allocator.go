//  Copyright 2022 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

package fsutil

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/pingcap/tiflow/pkg/redo"
)

// FileAllocator has two functionalities:
//  1. create new file or reuse the existing file (the existing tmp file will be cleared)
//     beforehand for file write.
//  2. pre-allocate disk space to mitigate the overhead of file metadata updating.
//
// ref: https://github.com/etcd-io/etcd/pull/4785
type FileAllocator struct {
	dir    string
	prefix string
	size   int64
	count  int
	wg     sync.WaitGroup
	// fileCh is unbuffered because we want only one file to be written next,
	// and another file to be standby.
	fileCh chan *os.File
	doneCh chan struct{}
	errCh  chan error
}

// NewFileAllocator creates a file allocator and starts a background file allocation goroutine.
func NewFileAllocator(dir string, prefix string, size int64) *FileAllocator {
	allocator := &FileAllocator{
		dir:    dir,
		prefix: prefix,
		size:   size,
		fileCh: make(chan *os.File),
		errCh:  make(chan error, 1),
		doneCh: make(chan struct{}),
	}

	allocator.wg.Add(1)
	go func() {
		defer allocator.wg.Done()
		allocator.run()
	}()

	return allocator
}

// Open returns a file for writing, this tmp file needs to be renamed after calling Open().
func (fl *FileAllocator) Open() (f *os.File, err error) {
	select {
	case f = <-fl.fileCh:
	case err = <-fl.errCh:
	}

	return f, err
}

// Close closes the doneCh to notify the background goroutine to exit.
func (fl *FileAllocator) Close() error {
	close(fl.doneCh)
	fl.wg.Wait()
	return <-fl.errCh
}

func (fl *FileAllocator) alloc() (f *os.File, err error) {
	if _, err := os.Stat(fl.dir); err != nil {
		return nil, err
	}

	filePath := filepath.Join(fl.dir, fmt.Sprintf("%s_%d.tmp", fl.prefix, fl.count%2))
	f, err = os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, redo.DefaultFileMode)
	if err != nil {
		return nil, err
	}
	err = PreAllocate(f, fl.size)
	if err != nil {
		return nil, err
	}
	fl.count++
	return f, nil
}

func (fl *FileAllocator) run() {
	defer close(fl.errCh)
	for {
		f, err := fl.alloc()
		if err != nil {
			fl.errCh <- err
			return
		}

		select {
		case fl.fileCh <- f:
		case <-fl.doneCh:
			os.Remove(f.Name())
			f.Close()
			return
		}
	}
}
