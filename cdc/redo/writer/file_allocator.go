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

package writer

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/pingcap/tiflow/cdc/redo/common"
	"github.com/pingcap/tiflow/pkg/fsutil"
)

// ref: https://github.com/etcd-io/etcd/pull/4785
// fileAllocator has two functionalities:
// 1. create new file or reuse the existing file in advance
//    before it will be written.
// 2. pre-allocate disk space to mitigate the overhead of file metadata updating.
type fileAllocator struct {
	dir      string
	fileType string
	size     int64
	count    int
	fileCh   chan *os.File
	errCh    chan error
	doneCh   chan struct{}
}

func newFileAllocator(dir string, fileType string, size int64) *fileAllocator {
	allocator := &fileAllocator{
		dir:      dir,
		fileType: fileType,
		size:     size,
		fileCh:   make(chan *os.File),
		errCh:    make(chan error),
		doneCh:   make(chan struct{}),
	}

	go allocator.run()

	return allocator
}

// Open returns a file for writing, this tmp file needs to be renamed after calling Open().
func (fl *fileAllocator) Open() (f *os.File, err error) {
	select {
	case f = <-fl.fileCh:
	case err = <-fl.errCh:
	}

	return f, err
}

// Close close the doneCh to notify the background goroutine to exit.
func (fl *fileAllocator) Close() error {
	close(fl.doneCh)
	return <-fl.errCh
}

func (fl *fileAllocator) alloc() (f *os.File, err error) {
	filePath := filepath.Join(fl.dir, fmt.Sprintf("%s_%d.tmp", fl.fileType, fl.count))
	f, err = os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, common.DefaultFileMode)
	if err != nil {
		return nil, err
	}
	err = fsutil.PreAllocate(f, fl.size)
	if err != nil {
		return nil, err
	}
	fl.count++
	return f, nil
}

func (fl *fileAllocator) run() {
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
