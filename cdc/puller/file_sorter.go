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

package puller

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/gob"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/pingcap/ticdc/cdc/model"
)

var (
	defaultSorterBufferSize        = 1000
	defaultAutoResolvedRows        = 1000
	defaultInitFileCount           = 10
	defaultFileSizeLimit    uint64 = 1 << 31 // 2GB per file at most
)

type fileCache struct {
	fileLock              sync.Mutex
	sorting               int32
	dir                   string
	toRemoveFiles         []string
	toRemoveUnsortedFiles []string
	unsortedFiles         []string
	lastSortedFile        string
	availableFileIdx      []int
	availableFileSize     map[int]uint64
}

func newFileCache(dir string) *fileCache {
	cache := &fileCache{
		dir:               dir,
		toRemoveFiles:     make([]string, 0, defaultInitFileCount),
		unsortedFiles:     make([]string, 0, defaultInitFileCount),
		availableFileIdx:  make([]int, 0, defaultInitFileCount),
		availableFileSize: make(map[int]uint64, defaultInitFileCount),
	}
	cache.extendUnsortFiles()
	return cache
}

func (cache *fileCache) resetUnsortedFiles() {
	cache.toRemoveUnsortedFiles = make([]string, len(cache.unsortedFiles))
	copy(cache.toRemoveUnsortedFiles, cache.unsortedFiles)
	cache.unsortedFiles = cache.unsortedFiles[:0]
	cache.availableFileIdx = cache.availableFileIdx[:0]
	cache.availableFileSize = make(map[int]uint64, defaultInitFileCount)
}

func (cache *fileCache) extendUnsortFiles() {
	fileCountBefore := len(cache.unsortedFiles)
	for i := fileCountBefore; i < fileCountBefore+defaultInitFileCount; i++ {
		cache.unsortedFiles = append(cache.unsortedFiles, randomFileName("unsorted"))
		cache.availableFileIdx = append(cache.availableFileIdx, i)
		cache.availableFileSize[i] = 0
	}
}

// next selects a random file from unsorted files which size is no more than defaultFileSizeLimit
// if no more available unsorted file, create some new unsorted files
func (cache *fileCache) next() (int, string) {
	if len(cache.availableFileIdx) == 0 {
		cache.extendUnsortFiles()
	}
	idx := rand.Intn(len(cache.availableFileIdx))
	return idx, cache.unsortedFiles[cache.availableFileIdx[idx]]
}

// increase records new file size of an unsorted file. If the file size exceeds
// defaultFileSizeLimit after increasing, remove this file from availableFileIdx
func (cache *fileCache) increase(idx, size int) {
	fileIdx := cache.availableFileIdx[idx]
	cache.availableFileSize[fileIdx] += uint64(size)
	if cache.availableFileSize[fileIdx] > defaultFileSizeLimit {
		cache.availableFileIdx = append(cache.availableFileIdx[:idx], cache.availableFileIdx[idx+1:]...)
		delete(cache.availableFileSize, fileIdx)
	}
}

func (cache *fileCache) gc() {
	// TODO: control gc running time, in case of the delete operation of
	// some large files blocks sorting
	cache.fileLock.Lock()
	defer cache.fileLock.Unlock()
	for _, f := range cache.toRemoveFiles {
		fpath := filepath.Join(cache.dir, f)
		if _, err := os.Stat(fpath); err == nil {
			err2 := os.Remove(fpath)
			if err2 != nil {
				log.Warn("remove file failed", zap.Error(err2))
			}
		}
	}
	cache.toRemoveFiles = cache.toRemoveFiles[:0]
}

// prepareSorting checks whether the file cache can start a new sorting round
// returns unsorted files list and whether the sorting can start
func (cache *fileCache) prepareSorting() ([]string, bool) {
	// clear and reset unsorted files, set cache sorting flag to prevent repeated sort
	cache.fileLock.Lock()
	defer cache.fileLock.Unlock()
	if atomic.LoadInt32(&cache.sorting) == 1 {
		return nil, false
	}
	atomic.StoreInt32(&cache.sorting, 1)
	files := make([]string, len(cache.unsortedFiles))
	copy(files, cache.unsortedFiles)
	cache.resetUnsortedFiles()
	return files, true
}

func (cache *fileCache) finishSorting(newLastSortedFile string, toRemoveFiles []string) {
	cache.fileLock.Lock()
	defer cache.fileLock.Unlock()
	atomic.StoreInt32(&cache.sorting, 0)
	cache.toRemoveFiles = append(cache.toRemoveFiles, toRemoveFiles...)
	cache.toRemoveFiles = append(cache.toRemoveFiles, cache.toRemoveUnsortedFiles...)
	cache.toRemoveUnsortedFiles = cache.toRemoveUnsortedFiles[:0]
	cache.lastSortedFile = newLastSortedFile
}

func (cache *fileCache) flush(ctx context.Context, entries []*model.PolymorphicEvent) error {
	cache.fileLock.Lock()
	defer cache.fileLock.Unlock()
	idx, filename := cache.next()
	fpath := filepath.Join(cache.dir, filename)
	dataLen, err := flushEventsToFile(ctx, fpath, entries)
	if err != nil {
		return errors.Trace(err)
	}
	cache.increase(idx, dataLen)
	return nil
}

// FileSorter accepts out-of-order raw kv entries, sort in local file system
// and output sorted entries
type FileSorter struct {
	dir      string
	outputCh chan *model.PolymorphicEvent
	inputCh  chan *model.PolymorphicEvent
	cache    *fileCache
}

// flushEventsToFile writes a slice of model.PolymorphicEvent to a given file in sequence
func flushEventsToFile(ctx context.Context, fullpath string, entries []*model.PolymorphicEvent) (int, error) {
	if len(entries) == 0 {
		return 0, nil
	}
	buf := new(bytes.Buffer)
	dataBuf := new(bytes.Buffer)
	var dataLen [8]byte
	for _, entry := range entries {
		err := entry.WaitPrepare(ctx)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if entry.Row == nil {
			continue
		}
		dataBuf.Reset()
		err = gob.NewEncoder(dataBuf).Encode(entry)
		if err != nil {
			return 0, errors.Trace(err)
		}
		binary.BigEndian.PutUint64(dataLen[:], uint64(dataBuf.Len()))
		buf.Write(dataLen[:])
		buf.Write(dataBuf.Bytes())
	}
	if buf.Len() == 0 {
		return 0, nil
	}
	f, err := os.OpenFile(fullpath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, errors.Trace(err)
	}
	w := bufio.NewWriter(f)
	_, err = w.Write(buf.Bytes())
	if err != nil {
		return 0, errors.Trace(err)
	}
	err = w.Flush()
	if err != nil {
		return 0, errors.Trace(err)
	}
	return buf.Len(), nil
}

// NewFileSorter creates a new FileSorter
func NewFileSorter(dir string) *FileSorter {
	fs := &FileSorter{
		dir:      dir,
		outputCh: make(chan *model.PolymorphicEvent, 128000),
		inputCh:  make(chan *model.PolymorphicEvent, 128000),
		cache:    newFileCache(dir),
	}
	return fs
}

// sortItem is used in PolymorphicEvent merge procedure from sorted files
type sortItem struct {
	entry     *model.PolymorphicEvent
	fileIndex int
	itemIndex int
}

type sortHeap []*sortItem

func (h sortHeap) Len() int           { return len(h) }
func (h sortHeap) Less(i, j int) bool { return h[i].entry.CRTs < h[j].entry.CRTs }
func (h sortHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *sortHeap) Push(x interface{}) {
	*h = append(*h, x.(*sortItem))
}
func (h *sortHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[0 : n-1]
	return x
}

const defaultBatchSize = 8 * 1024 * 2014

// readPolymorphicEvent reads PolymorphicEvents from file reader and also advance reader
// TODO: batch read
func readPolymorphicEvent(rd *bufio.Reader, readBuf *bytes.Reader, batchSize int) ([]*model.PolymorphicEvent, error) {
	var readSize int
	var byteLen [8]byte
	buf := make([]byte, 128)
	evs := make([]*model.PolymorphicEvent, 0, batchSize/128)

	getBuf := func(dataLen int) []byte {
		if dataLen > cap(buf) {
			buf = append(buf, make([]byte, dataLen-cap(buf))...)
		}
		return buf[:dataLen]
	}
	for readSize < batchSize {
		n, err := io.ReadFull(rd, byteLen[:])
		if err != nil {
			if err == io.EOF {
				return evs, nil
			}
			return nil, errors.Trace(err)
		}
		if n < 8 {
			return nil, errors.Errorf("invalid length data %s, read %d bytes", byteLen, n)
		}
		dataLen := int(binary.BigEndian.Uint64(byteLen[:]))
		data := getBuf(dataLen)
		n, err = io.ReadFull(rd, data)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if n != dataLen {
			return nil, errors.Errorf("truncated data %s n: %d dataLen: %d", data, n, dataLen)
		}

		readBuf.Reset(data)
		ev := &model.PolymorphicEvent{}
		err = gob.NewDecoder(readBuf).Decode(ev)
		if err != nil {
			return nil, errors.Trace(err)
		}
		evs = append(evs, ev)
		readSize += 8 + dataLen
	}
	return evs, nil
}

func (fs *FileSorter) output(ctx context.Context, entry *model.PolymorphicEvent) {
	select {
	case <-ctx.Done():
		return
	case fs.outputCh <- entry:
	}
}

func (fs *FileSorter) rotate(ctx context.Context, resolvedTs uint64) error {
	// sortSingleFile reads an unsorted file into memory, sort in memory and rewritten
	// sorted events ta a new file.
	sortSingleFile := func(ctx context.Context, filename string) (string, error) {
		fpath := filepath.Join(fs.dir, filename)
		_, err := os.Stat(fpath)
		if os.IsNotExist(err) {
			return "", nil
		}
		data, err := ioutil.ReadFile(fpath)
		if err != nil {
			return "", errors.Trace(err)
		}
		evs := make([]*model.PolymorphicEvent, 0)
		idx := 0
		reader := new(bytes.Reader)
		for idx < len(data) {
			dataLen := int(binary.BigEndian.Uint64(data[idx : idx+8]))
			if idx+8+dataLen > len(data) {
				return "", errors.New("unsorted file unexpected truncated")
			}
			ev := &model.PolymorphicEvent{}
			reader.Reset(data[idx+8 : idx+8+dataLen])
			err = gob.NewDecoder(reader).Decode(ev)

			if err != nil {
				return "", errors.Trace(err)
			}
			evs = append(evs, ev)
			idx = idx + 8 + dataLen
		}
		// event count in unsorted file may be zero
		if len(evs) == 0 {
			return "", nil
		}
		sort.Slice(evs, func(i, j int) bool {
			return evs[i].CRTs < evs[j].CRTs
		})
		newfile := randomFileName("sorted")
		newfpath := filepath.Join(fs.dir, newfile)
		buffer := make([]*model.PolymorphicEvent, 0, defaultSorterBufferSize)
		for _, entry := range evs {
			buffer = append(buffer, entry)
			if len(buffer) >= defaultSorterBufferSize {
				_, err := flushEventsToFile(ctx, newfpath, buffer)
				if err != nil {
					return "", errors.Trace(err)
				}
				buffer = buffer[:0]
			}
		}
		if len(buffer) > 0 {
			_, err := flushEventsToFile(ctx, newfpath, buffer)
			if err != nil {
				return "", errors.Trace(err)
			}
		}
		return newfile, nil
	}

	files, start := fs.cache.prepareSorting()
	if !start {
		return nil
	}

	// prepare buffer reader of all sorted files
	readers := make([]*bufio.Reader, 0, len(files)+1)
	toRemoveFiles := make([]string, 0, len(files)+1)
	for _, f := range files {
		sortedFile, err := sortSingleFile(ctx, f)
		if err != nil {
			return errors.Trace(err)
		}
		if sortedFile == "" {
			continue
		}
		toRemoveFiles = append(toRemoveFiles, sortedFile)
		fd, err := os.Open(filepath.Join(fs.dir, sortedFile))
		if err != nil {
			return errors.Trace(err)
		}
		rd := bufio.NewReader(fd)
		readers = append(readers, rd)
	}
	if fs.cache.lastSortedFile != "" {
		toRemoveFiles = append(toRemoveFiles, fs.cache.lastSortedFile)
		fd, err := os.Open(filepath.Join(fs.dir, fs.cache.lastSortedFile))
		if err != nil {
			return errors.Trace(err)
		}
		rd := bufio.NewReader(fd)
		readers = append(readers, rd)
	}


	// merge data from all sorted files, output events with ts less than resolvedTs,
	// the rest events will be rewritten into the new lastSortedFile
	h := &sortHeap{}
	heap.Init(h)
	readBuf := new(bytes.Reader)
	itemLastIdexs := make([]int, len(readers))
	rowCount := 0
	for i, fd := range readers {
		evs, err := readPolymorphicEvent(fd, readBuf, defaultBatchSize)
		if err != nil {
			return errors.Trace(err)
		}
		if len(evs) == 0 {
			continue
		}
		for j, ev := range evs {
			heap.Push(h, &sortItem{entry: ev, fileIndex: i, itemIndex:j})
		}
		itemLastIdexs[i] = len(evs) - 1
	}
	lastSortedFileUpdated := false
	newLastSortedFile := randomFileName("last-sorted")
	buffer := make([]*model.PolymorphicEvent, 0, defaultSorterBufferSize)
	for h.Len() > 0 {
		item := heap.Pop(h).(*sortItem)
		if item.entry.CRTs <= resolvedTs {
			fs.output(ctx, item.entry)
			// As events are sorted, we can output a resolved ts at any time.
			// If we don't output a resovled ts event, the processor will still
			// cache all events in memory until it receives the resolved ts when
			// file sorter outputs all events in this rotate round.
			// Events after this one could have the same commit ts with
			// `item.entry.CRTs`, so we can't output a resolved event with
			// `item.entry.CRTs`. But it is safe to output with `item.entry.CRTs-1`.
			rowCount += 1
			if rowCount%defaultAutoResolvedRows == 0 {
				fs.output(ctx, model.NewResolvedPolymorphicEvent(item.entry.CRTs-1))
			}
		} else {
			lastSortedFileUpdated = true
			buffer = append(buffer, item.entry)
			if len(buffer) > defaultSorterBufferSize {
				_, err := flushEventsToFile(ctx, filepath.Join(fs.dir, newLastSortedFile), buffer)
				if err != nil {
					return errors.Trace(err)
				}
				buffer = buffer[:0]
			}
		}
		if item.itemIndex == itemLastIdexs[item.fileIndex] {
			evs, err := readPolymorphicEvent(readers[item.fileIndex], readBuf, defaultBatchSize)
			if err != nil {
				return errors.Trace(err)
			}
			if len(evs) == 0{
				// all events in this file have been consumed
				continue
			}
			for i, ev := range evs {
				heap.Push(h, &sortItem{entry: ev, fileIndex: item.fileIndex, itemIndex:itemLastIdexs[item.fileIndex] + 1 + i})
			}
			itemLastIdexs[item.fileIndex] += len(evs)
		}
	}
	if len(buffer) > 0 {
		_, err := flushEventsToFile(ctx, filepath.Join(fs.dir, newLastSortedFile), buffer)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if !lastSortedFileUpdated {
		newLastSortedFile = ""
	}

	fs.cache.finishSorting(newLastSortedFile, toRemoveFiles)
	fs.output(ctx, model.NewResolvedPolymorphicEvent(resolvedTs))

	return nil
}

// AddEntry adds an RawKVEntry to file sorter cache
func (fs *FileSorter) AddEntry(ctx context.Context, entry *model.PolymorphicEvent) {
	select {
	case <-ctx.Done():
		return
	case fs.inputCh <- entry:
	}
}

// Output returns the sorted PolymorphicEvent in output channel
func (fs *FileSorter) Output() <-chan *model.PolymorphicEvent {
	return fs.outputCh
}

// Run implements EventSorter.Run, runs in background, sorts and sends sorted events to output channel
func (fs *FileSorter) Run(ctx context.Context) error {
	wg, ctx := errgroup.WithContext(ctx)

	wg.Go(func() error {
		return fs.sortAndOutput(ctx)
	})

	wg.Go(func() error {
		return fs.gcRemovedFiles(ctx)
	})

	return wg.Wait()
}

func (fs *FileSorter) sortAndOutput(ctx context.Context) error {
	buffer := make([]*model.PolymorphicEvent, 0, defaultSorterBufferSize)

	flush := func() error {
		err := fs.cache.flush(ctx, buffer)
		if err != nil {
			return errors.Trace(err)
		}
		buffer = buffer[:0]
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case ev := <-fs.inputCh:
			if ev.RawKV.OpType == model.OpTypeResolved {
				err := flush()
				if err != nil {
					return errors.Trace(err)
				}
				err = fs.rotate(ctx, ev.RawKV.CRTs)
				if err != nil {
					return errors.Trace(err)
				}
				continue
			}
			buffer = append(buffer, ev)
			if len(buffer) >= defaultSorterBufferSize {
				err := flush()
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	}
}

func (fs *FileSorter) gcRemovedFiles(ctx context.Context) error {
	ticker := time.NewTicker(time.Minute)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			fs.cache.gc()
		}
	}
}

func randomFileName(prefix string) string {
	return prefix + "-" + uuid.New().String()
}
