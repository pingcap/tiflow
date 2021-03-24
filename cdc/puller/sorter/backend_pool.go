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
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/ticdc/pkg/util"

	"github.com/mackerelio/go-osstat/memory"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

const (
	backgroundJobInterval = time.Second * 15
)

var (
	pool   *backEndPool // this is the singleton instance of backEndPool
	poolMu sync.Mutex   // this mutex is for delayed initialization of `pool` only
)

type backEndPool struct {
	memoryUseEstimate int64
	onDiskDataSize    int64
	fileNameCounter   uint64
	memPressure       int32
	cache             [256]unsafe.Pointer
	dir               string
	filePrefix        string

	// cancelCh needs to be unbuffered to prevent races
	cancelCh chan struct{}
	// cancelRWLock protects cache against races when the backEnd is exiting
	cancelRWLock  sync.RWMutex
	isTerminating bool
}

func newBackEndPool(dir string, captureAddr string) *backEndPool {
	ret := &backEndPool{
		memoryUseEstimate: 0,
		fileNameCounter:   0,
		dir:               dir,
		cancelCh:          make(chan struct{}),
		filePrefix:        fmt.Sprintf("%s/sort-%d-", dir, os.Getpid()),
	}

	go func() {
		ticker := time.NewTicker(backgroundJobInterval)
		defer ticker.Stop()

		metricSorterInMemoryDataSizeGauge := sorterInMemoryDataSizeGauge.WithLabelValues(captureAddr)
		metricSorterOnDiskDataSizeGauge := sorterOnDiskDataSizeGauge.WithLabelValues(captureAddr)
		metricSorterOpenFileCountGauge := sorterOpenFileCountGauge.WithLabelValues(captureAddr)

		for {
			select {
			case <-ret.cancelCh:
				log.Info("Unified Sorter backEnd is being cancelled")
				return
			case <-ticker.C:
			}

			metricSorterInMemoryDataSizeGauge.Set(float64(atomic.LoadInt64(&ret.memoryUseEstimate)))
			metricSorterOnDiskDataSizeGauge.Set(float64(atomic.LoadInt64(&ret.onDiskDataSize)))
			metricSorterOpenFileCountGauge.Set(float64(atomic.LoadInt64(&openFDCount)))

			// update memPressure
			m, err := memory.Get()
			if err != nil {
				failpoint.Inject("sorterDebug", func() {
					log.Panic("unified sorter: getting system memory usage failed", zap.Error(err))
				})

				log.Warn("unified sorter: getting system memory usage failed", zap.Error(err))
			}

			memPressure := m.Used * 100 / m.Total
			atomic.StoreInt32(&ret.memPressure, int32(memPressure))

			if memPressure := ret.memoryPressure(); memPressure > 50 {
				log.Debug("unified sorter: high memory pressure", zap.Int32("memPressure", memPressure),
					zap.Int64("usedBySorter", ret.sorterMemoryUsage()))
				// Increase GC frequency to avoid unnecessary OOMs
				debug.SetGCPercent(10)
				if memPressure > 80 {
					runtime.GC()
				}
			} else {
				debug.SetGCPercent(50)
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
	sorterConfig := config.GetGlobalServerConfig().Sorter
	if p.sorterMemoryUsage() < int64(sorterConfig.MaxMemoryConsumption) &&
		p.memoryPressure() < int32(sorterConfig.MaxMemoryPressure) {

		ret := newMemoryBackEnd()
		return ret, nil
	}

	p.cancelRWLock.RLock()
	defer p.cancelRWLock.RUnlock()

	if p.isTerminating {
		return nil, cerrors.ErrUnifiedSorterBackendTerminating.GenWithStackByArgs()
	}

	for i := range p.cache {
		ptr := &p.cache[i]
		ret := atomic.SwapPointer(ptr, nil)
		if ret != nil {
			return (*fileBackEnd)(ret), nil
		}
	}

	fname := fmt.Sprintf("%s%d.tmp", p.filePrefix, atomic.AddUint64(&p.fileNameCounter, 1))
	tableID, tableName := util.TableIDFromCtx(ctx)
	log.Debug("Unified Sorter: trying to create file backEnd",
		zap.String("filename", fname),
		zap.Int64("table-id", tableID),
		zap.String("table-name", tableName))

	ret, err := newFileBackEnd(fname, &msgPackGenSerde{})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return ret, nil
}

func (p *backEndPool) dealloc(backEnd backEnd) error {
	switch b := backEnd.(type) {
	case *memoryBackEnd:
		err := b.free()
		if err != nil {
			log.Warn("error freeing memory backend", zap.Error(err))
		}
		// Let GC do its job
		return nil
	case *fileBackEnd:
		failpoint.Inject("sorterDebug", func() {
			if atomic.LoadInt32(&b.borrowed) != 0 {
				log.Warn("Deallocating a fileBackEnd in use", zap.String("filename", b.fileName))
				failpoint.Return(nil)
			}
		})

		b.cleanStats()

		p.cancelRWLock.RLock()
		defer p.cancelRWLock.RUnlock()

		if p.isTerminating {
			return cerrors.ErrUnifiedSorterBackendTerminating.GenWithStackByArgs()
		}

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
		log.Panic("backEndPool: unexpected backEnd type to be deallocated", zap.Reflect("type", reflect.TypeOf(backEnd)))
	}
	return nil
}

func (p *backEndPool) terminate() {
	p.cancelCh <- struct{}{}
	defer close(p.cancelCh)
	// the background goroutine can be considered terminated here

	p.cancelRWLock.Lock()
	defer p.cancelRWLock.Unlock()
	p.isTerminating = true

	// any new allocs and deallocs will not succeed from this point
	// accessing p.cache without atomics is safe from now

	for i := range p.cache {
		ptr := &p.cache[i]
		backend := (*fileBackEnd)(*ptr)
		if backend == nil {
			continue
		}
		_ = backend.free()
	}

	if p.filePrefix == "" {
		// This should not happen. But to prevent accidents in production, we add this anyway.
		log.Panic("Empty filePrefix, please report a bug")
	}

	files, err := filepath.Glob(p.filePrefix + "*")
	if err != nil {
		log.Warn("Unified Sorter clean-up failed", zap.Error(err))
	}
	for _, file := range files {
		err = os.RemoveAll(file)
		if err != nil {
			log.Warn("Unified Sorter clean-up failed: failed to remove", zap.String("file-name", file), zap.Error(err))
		}
	}
}

func (p *backEndPool) sorterMemoryUsage() int64 {
	failpoint.Inject("memoryUsageInjectPoint", func(val failpoint.Value) {
		failpoint.Return(int64(val.(int)))
	})
	return atomic.LoadInt64(&p.memoryUseEstimate)
}

func (p *backEndPool) memoryPressure() int32 {
	failpoint.Inject("memoryPressureInjectPoint", func(val failpoint.Value) {
		failpoint.Return(int32(val.(int)))
	})
	return atomic.LoadInt32(&p.memPressure)
}
