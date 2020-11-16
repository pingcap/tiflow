// Copyright 2020 PingCAP, Inc.
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

package sorter

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/mackerelio/go-osstat/memory"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	"go.uber.org/zap"
)


var (
	pool   *backEndPool
	poolMu sync.Mutex   // this mutex is for delayed initialization of `pool` only
)

type backEndPool struct {
	memoryUseEstimate int64
	fileNameCounter   uint64
	memPressure       int32
	cache             [256]unsafe.Pointer
	dir               string
}

func newBackEndPool(dir string) *backEndPool {
	ret := &backEndPool{
		memoryUseEstimate: 0,
		fileNameCounter:   0,
		dir:               dir,
	}

	go func() {
		ticker := time.NewTicker(5 * time.Second)

		for {
			<-ticker.C

			// update memPressure
			m, err := memory.Get()
			if err != nil {
				failpoint.Inject("sorterDebug", func() {
					log.Fatal("unified sorter: getting system memory usage failed", zap.Error(err))
				})

				log.Warn("unified sorter: getting system memory usage failed", zap.Error(err))
			}

			memPressure := m.Used * 100 / m.Total
			atomic.StoreInt32(&ret.memPressure, int32(memPressure))
			if memPressure > 50 {
				log.Debug("unified sorter: high memory pressure", zap.Uint64("memPressure", memPressure))
				// Increase GC frequency to avoid necessary OOM
				debug.SetGCPercent(10)
			} else {
				debug.SetGCPercent(100)
			}

			// garbage collect temporary files in batches
			freedCount := 0
			for i := range ret.cache {
				ptr := &ret.cache[i]
				innerPtr := atomic.SwapPointer(ptr, nil)
				if innerPtr == nil {
					continue
				}
				backEnd := (*fileBackEnd)(innerPtr)
				err := backEnd.free()
				if err != nil {
					log.Warn("Cannot remove temporary file for sorting", zap.String("file", backEnd.fileName), zap.Error(err))
				} else {
					log.Info("Temporary file removed", zap.String("file", backEnd.fileName))
					freedCount += 1
				}
				if freedCount >= 16 {
					freedCount = 0
					break
				}
			}
		}
	}()

	return ret
}

func (p *backEndPool) alloc(ctx context.Context) (backEnd, error) {
	sorterConfig := config.GetSorterConfig()
	if atomic.LoadInt64(&p.memoryUseEstimate) < int64(sorterConfig.MaxMemoryConsumption) &&
		atomic.LoadInt32(&p.memPressure) < int32(sorterConfig.MaxMemoryPressure) {

		ret := newMemoryBackEnd()
		return ret, nil
	}

	for i := range p.cache {
		ptr := &p.cache[i]
		ret := atomic.SwapPointer(ptr, nil)
		if ret != nil {
			return (*fileBackEnd)(ret), nil
		}
	}

	fname := fmt.Sprintf("%s/sort-%d-%d", p.dir, os.Getpid(), atomic.AddUint64(&p.fileNameCounter, 1))
	log.Debug("Unified Sorter: trying to create file backEnd", zap.String("filename", fname))

	ret, err := newFileBackEnd(fname, &msgPackGenSerde{})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return ret, nil
}

func (p *backEndPool) dealloc(backEnd backEnd) error {
	switch b := backEnd.(type) {
	case *memoryBackEnd:
		// Let GC do its job
		return nil
	case *fileBackEnd:
		for i := range p.cache {
			ptr := &p.cache[i]
			if atomic.CompareAndSwapPointer(ptr, nil, unsafe.Pointer(b)) {
				return nil
			}
		}
		// Cache is full.
		err := b.free()
		if err != nil {
			return errors.Trace(err)
		}

		return nil
	default:
		log.Fatal("backEndPool: unexpected backEnd type to be deallocated", zap.Reflect("type", reflect.TypeOf(backEnd)))
	}
	return nil
}
