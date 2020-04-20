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
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	defaultInitFileCount        = 10
	defaultFileSizeLimit uint64 = 1 << 31 // 2GB per file at most
)

type fileCache struct {
	fileLock          sync.Mutex
	sorting           int32
	toRemoveFiles     []string
	unsortedFiles     []string
	lastSortedFile    string
	availableFileIdx  []int
	availableFileSize map[int]uint64
}

func newFileCache() *fileCache {
	cache := &fileCache{
		toRemoveFiles:     make([]string, 0, defaultInitFileCount),
		unsortedFiles:     make([]string, 0, defaultInitFileCount),
		availableFileIdx:  make([]int, 0, defaultInitFileCount),
		availableFileSize: make(map[int]uint64, defaultInitFileCount),
	}
	cache.extendUnsortFiles()
	return cache
}

func (cache *fileCache) resetUnsortedFiles() {
	cache.toRemoveFiles = append(cache.toRemoveFiles, cache.unsortedFiles...)
	cache.unsortedFiles = make([]string, 0, defaultInitFileCount)
	cache.availableFileIdx = make([]int, 0, defaultInitFileCount)
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
		dataBuf.Reset()
		err = gob.NewEncoder(dataBuf).Encode(entry)
		if err != nil {
			return 0, errors.Trace(err)
		}
		binary.BigEndian.PutUint64(dataLen[:], uint64(dataBuf.Len()))
		buf.Write(dataLen[:])
		buf.Write(dataBuf.Bytes())
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
		cache:    newFileCache(),
	}
	return fs
}

// flush writes a slice of model.PolymorphicEvent to a random unsorted file in sequence
func (fs *FileSorter) flush(ctx context.Context, entries []*model.PolymorphicEvent) error {
	fs.cache.fileLock.Lock()
	defer fs.cache.fileLock.Unlock()
	idx, filename := fs.cache.next()
	fpath := filepath.Join(fs.dir, filename)
	dataLen, err := flushEventsToFile(ctx, fpath, entries)
	if err != nil {
		return errors.Trace(err)
	}
	fs.cache.increase(idx, dataLen)
	return nil
}

// sortItem is used in PolymorphicEvent merge procedure from sorted files
type sortItem struct {
	entry     *model.PolymorphicEvent
	fileIndex int
}

type sortHeap []*sortItem

func (h sortHeap) Len() int           { return len(h) }
func (h sortHeap) Less(i, j int) bool { return h[i].entry.Ts < h[j].entry.Ts }
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

// readPolymorphicEvent reads a PolymorphicEvent from file reader and also advance reader
// TODO: batch read
func readPolymorphicEvent(rd *bufio.Reader) (*model.PolymorphicEvent, error) {
	var byteLen [8]byte
	n, err := rd.Read(byteLen[:])
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, errors.Trace(err)
	}
	if n < 8 {
		return nil, errors.Errorf("invalid lenth data %s", byteLen)
	}
	dataLen := int(binary.BigEndian.Uint64(byteLen[:]))

	data := make([]byte, dataLen)
	n, err = io.ReadFull(rd, data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if n != dataLen {
		return nil, errors.Errorf("truncated data %s n: %d dataLen: %d", data, n, dataLen)
	}

	ev := &model.PolymorphicEvent{}
	err = gob.NewDecoder(bytes.NewReader(data)).Decode(ev)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return ev, nil
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
		for idx < len(data) {
			dataLen := int(binary.BigEndian.Uint64(data[idx : idx+8]))
			if idx+8+dataLen > len(data) {
				return "", errors.New("unsorted file unexpected truncated")
			}
			ev := &model.PolymorphicEvent{}
			err = gob.NewDecoder(bytes.NewReader(data[idx+8 : idx+8+dataLen])).Decode(ev)

			if err != nil {
				return "", errors.Trace(err)
			}
			evs = append(evs, ev)
			idx = idx + 8 + dataLen
		}
		sort.Slice(evs, func(i, j int) bool {
			return evs[i].Ts < evs[j].Ts
		})
		batchFlushSize := 10
		newfile := randomFileName("sorted")
		newfpath := filepath.Join(fs.dir, newfile)
		buffer := make([]*model.PolymorphicEvent, 0, batchFlushSize)
		for _, entry := range evs {
			buffer = append(buffer, entry)
			if len(buffer) >= batchFlushSize {
				_, err := flushEventsToFile(ctx, newfpath, buffer)
				if err != nil {
					return "", errors.Trace(err)
				}
				buffer = make([]*model.PolymorphicEvent, 0, batchFlushSize)
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

	// clear and reset unsorted files, set cache sorting flag to prevent repeated sort
	fs.cache.fileLock.Lock()
	if atomic.LoadInt32(&fs.cache.sorting) == 1 {
		fs.cache.fileLock.Unlock()
		return nil
	}
	atomic.StoreInt32(&fs.cache.sorting, 1)
	files := make([]string, len(fs.cache.unsortedFiles))
	copy(files, fs.cache.unsortedFiles)
	fs.cache.resetUnsortedFiles()
	fs.cache.fileLock.Unlock()

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
	for i, fd := range readers {
		ev, err := readPolymorphicEvent(fd)
		if err != nil {
			return errors.Trace(err)
		}
		if ev == nil {
			continue
		}
		heap.Push(h, &sortItem{entry: ev, fileIndex: i})
	}
	lastSortedFileHasData := false
	newLastSortedFile := randomFileName("last-sorted")
	bufferLen := 10
	buffer := make([]*model.PolymorphicEvent, 0, bufferLen)
	for h.Len() > 0 {
		item := heap.Pop(h).(*sortItem)
		if item.entry.Ts <= resolvedTs {
			fs.output(ctx, item.entry)
		} else {
			lastSortedFileHasData = true
			buffer = append(buffer, item.entry)
			if len(buffer) > bufferLen {
				_, err := flushEventsToFile(ctx, filepath.Join(fs.dir, newLastSortedFile), buffer)
				if err != nil {
					return errors.Trace(err)
				}
				buffer = make([]*model.PolymorphicEvent, 0, bufferLen)
			}
		}
		ev, err := readPolymorphicEvent(readers[item.fileIndex])
		if err != nil {
			return errors.Trace(err)
		}
		if ev == nil {
			// all events in this file have been consumed
			continue
		}
		heap.Push(h, &sortItem{entry: ev, fileIndex: item.fileIndex})
	}
	if len(buffer) > 0 {
		_, err := flushEventsToFile(ctx, filepath.Join(fs.dir, newLastSortedFile), buffer)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if lastSortedFileHasData {
		fs.cache.lastSortedFile = newLastSortedFile
	} else {
		fs.cache.lastSortedFile = ""
	}

	fs.cache.fileLock.Lock()
	atomic.StoreInt32(&fs.cache.sorting, 0)
	fs.cache.toRemoveFiles = append(fs.cache.toRemoveFiles, toRemoveFiles...)
	fs.cache.fileLock.Unlock()
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
	bufferLen := 10
	buffer := make([]*model.PolymorphicEvent, 0, bufferLen)

	flush := func() error {
		err := fs.flush(ctx, buffer)
		if err != nil {
			return errors.Trace(err)
		}
		buffer = make([]*model.PolymorphicEvent, 0, bufferLen)
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
				err = fs.rotate(ctx, ev.RawKV.Ts)
				if err != nil {
					return errors.Trace(err)
				}
				continue
			}
			buffer = append(buffer, ev)
			if len(buffer) >= bufferLen {
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
			// TODO: control gc running time, in case of the delete operation of
			// some large files blocks sorting
			fs.cache.fileLock.Lock()
			for _, f := range fs.cache.toRemoveFiles {
				fpath := filepath.Join(fs.dir, f)
				err := os.Remove(fpath)
				if err != nil {
					log.Warn("remove file failed", zap.Error(err))
				}
			}
			fs.cache.toRemoveFiles = make([]string, 0)
			fs.cache.fileLock.Unlock()
		}
	}
}

func randomFileName(prefix string) string {
	return prefix + "-" + uuid.New().String()
}
