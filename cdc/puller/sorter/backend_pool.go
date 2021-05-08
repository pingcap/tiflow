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

	"github.com/mackerelio/go-osstat/memory"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filelock"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

const (
	backgroundJobInterval      = time.Second * 15
	sortDirLockFileName        = "ticdc_lock"
	sortDirDataFileMagicPrefix = "sort"
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

	// to prevent `dir` from being accidentally used by another TiCDC server process.
	fileLock *filelock.FileLock

	// cancelCh needs to be unbuffered to prevent races
	cancelCh chan struct{}
	// cancelRWLock protects cache against races when the backEnd is exiting
	cancelRWLock  sync.RWMutex
	isTerminating bool
}

func newBackEndPool(dir string, captureAddr string) (*backEndPool, error) {
	ret := &backEndPool{
		memoryUseEstimate: 0,
		fileNameCounter:   0,
		dir:               dir,
		cancelCh:          make(chan struct{}),
		filePrefix:        fmt.Sprintf("%s/%s-%d-", dir, sortDirDataFileMagicPrefix, os.Getpid()),
	}

	err := ret.lockSortDir()
	if err != nil {
		log.Warn("failed to lock file prefix",
			zap.String("prefix", ret.filePrefix),
			zap.Error(err))
		return nil, errors.Trace(err)
	}

	err = ret.cleanUpStaleFiles()
	if err != nil {
		log.Warn("Unified Sorter: failed to clean up stale temporary files. Report a bug if you believe this is unexpected", zap.Error(err))
		return nil, errors.Trace(err)
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

	return ret, nil
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
	defer func() {
		if p.fileLock == nil {
			return
		}
		err := p.unlockSortDir()
		if err != nil {
			log.Warn("failed to unlock file prefix", zap.String("prefix", p.filePrefix))
		}
	}()

	p.cancelCh <- struct{}{}
	defer close(p.cancelCh)
	// the background goroutine can be considered terminated here

	log.Debug("Unified Sorter terminating...")
	p.cancelRWLock.Lock()
	defer p.cancelRWLock.Unlock()
	p.isTerminating = true

	log.Debug("Unified Sorter cleaning up before exiting")
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
		log.Debug("Unified Sorter backEnd removing file", zap.String("file", file))
		err = os.RemoveAll(file)
		if err != nil {
			log.Warn("Unified Sorter clean-up failed: failed to remove", zap.String("file-name", file), zap.Error(err))
		}
	}

	log.Debug("Unified Sorter backEnd terminated")
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

func (p *backEndPool) lockSortDir() error {
	lockFileName := fmt.Sprintf("%s/%s", p.dir, sortDirLockFileName)
	fileLock, err := filelock.NewFileLock(lockFileName)
	if err != nil {
		return cerrors.ErrSortDirLockError.Wrap(err).GenWithStackByCause()
	}

	err = fileLock.Lock()
	if err != nil {
		if cerrors.ErrConflictingFileLocks.Equal(err) {
			log.Warn("TiCDC failed to lock sorter temporary file directory. "+
				"Make sure that another instance of TiCDC, or any other program, is not using the directory. "+
				"If you believe you should not see this error, try deleting the lock file and resume the changefeed. "+
				"Report a bug or contact support if the problem persists.",
				zap.String("lock-file", lockFileName))
			return errors.Trace(err)
		}
		return cerrors.ErrSortDirLockError.Wrap(err).GenWithStackByCause()
	}

	p.fileLock = fileLock
	return nil
}

func (p *backEndPool) unlockSortDir() error {
	err := p.fileLock.Unlock()
	if err != nil {
		return cerrors.ErrSortDirLockError.Wrap(err).FastGenWithCause()
	}
	return nil
}

func (p *backEndPool) cleanUpStaleFiles() error {
	if p.dir == "" {
		// guard against programmer error. Must be careful when we are deleting user files.
		log.Panic("unexpected sort-dir", zap.String("sort-dir", p.dir))
	}

	files, err := filepath.Glob(filepath.Join(p.dir, fmt.Sprintf("%s-*", sortDirDataFileMagicPrefix)))
	if err != nil {
		return errors.Trace(err)
	}

	for _, toRemoveFilePath := range files {
		log.Info("Removing stale sorter temporary file", zap.String("file", toRemoveFilePath))
		err := os.Remove(toRemoveFilePath)
		if err != nil {
			// In production, we do not want an error here to interfere with normal operation,
			// because in most situations, failure to remove files only indicates non-fatal misconfigurations
			// such as permission problems, rather than fatal errors.
			// If the directory is truly unusable, other errors would be raised when we try to write to it.
			log.Warn("failed to remove file",
				zap.String("file", toRemoveFilePath),
				zap.Error(err))
			// For fail-fast in integration tests
			failpoint.Inject("sorterDebug", func() {
				log.Panic("panicking", zap.Error(err))
			})
		}
	}

	return nil
}
