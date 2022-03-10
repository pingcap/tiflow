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

package verification

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/db"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/atomic"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type Module uint32

//go:generate stringer -type=Module
const (
	Puller Module = iota
	Sorter
	Cyclic
	Sink
)

const (
	// Write batch size should be larger than block size to save CPU.
	writeBatchSizeFactor = 16
)

//go:generate mockery --name=ModuleVerifier --inpackage
type ModuleVerifier interface {
	// SentTrackData sent the track data to verification, the data should send according to the order if any. data is FIFO guaranteed, in the same changefeed, same module
	SentTrackData(ctx context.Context, module Module, data []TrackData)
	// Verify run the module level consistency check base on the time range [startTs, endTs]
	Verify(ctx context.Context, startTs, endTs string) error
	// GC clean the checked trackData
	GC(endTs string) error
	// Close clean related resource
	Close() error
}

type ModuleVerification struct {
	cfg *ModuleVerificationConfig
	db  db.DB
	// write-only batch
	wb           db.Batch
	commitMu     sync.Mutex
	committing   atomic.Bool
	deleteCount  uint64
	nextDeleteTs time.Time
}

type ModuleVerificationConfig struct {
	ChangeFeedID        string
	DBConfig            *config.DBConfig
	MaxMemoryPercentage int
	CyclicEnable        bool
}

var (
	verifications = map[string]*ModuleVerification{}
	initLock      sync.Mutex
)

// NewModuleVerification return the module level verification per changefeed
func NewModuleVerification(ctx context.Context, cfg *ModuleVerificationConfig) (*ModuleVerification, error) {
	if cfg == nil {
		return nil, cerror.WrapError(cerror.ErrVerificationConfigInvalid, errors.New("ModuleVerificationConfig can not be nil"))
	}

	initLock.Lock()
	defer initLock.Unlock()

	if v, ok := verifications[cfg.ChangeFeedID]; ok {
		return v, nil
	}

	totalMemory, err := memory.MemTotal()
	if err != nil {
		return nil, err
	}
	if cfg.MaxMemoryPercentage == 0 {
		cfg.MaxMemoryPercentage = 20
	}
	memPercentage := float64(cfg.MaxMemoryPercentage) / 100
	memInByte := float64(totalMemory) * memPercentage

	dir := filepath.Join(config.GetDefaultServerConfig().DataDir, config.DefaultVerificationDir, cfg.ChangeFeedID)
	if cfg.DBConfig == nil {
		cfg.DBConfig = config.GetDefaultServerConfig().Debug.DB
	}
	// TODO: meaningful id value?
	pebble, err := db.OpenPebble(ctx, 0, dir, int(memInByte), cfg.DBConfig)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrPebbleDBError, err)
	}

	wb := pebble.Batch(0)
	m := &ModuleVerification{
		db: pebble,
		wb: wb,
	}
	verifications[cfg.ChangeFeedID] = m

	return m, nil
}

var (
	preTsList = map[string]uint64{}
	preLock   sync.RWMutex
)

type TrackData struct {
	TrackID  []byte
	CommitTs uint64
}

// SentTrackData implement SentTrackData api
func (m *ModuleVerification) SentTrackData(ctx context.Context, module Module, data []TrackData) {
	select {
	case <-ctx.Done():
		log.Info("SentTrackData ctx cancel", zap.Error(ctx.Err()))
		return
	default:
	}

	// wait for commit complete
	if m.committing.Load() {
		m.commitMu.Lock()
		m.commitMu.Unlock()
	}

	var preTs uint64 = 0
	var tsKey string
	if checkOrder(module) {
		preLock.RLock()
		tsKey = m.generatePreTsKey(module)
		if v, ok := preTsList[tsKey]; ok {
			preTs = v
		}
		preLock.RUnlock()
	}

	for _, datum := range data {
		if checkOrder(module) {
			if datum.CommitTs < preTs {
				log.Error("module level verify fail, the commitTs of the data is less than previous data",
					zap.String("module", module.String()),
					zap.Any("trackData", data),
					zap.Uint64("preTs", preTs),
					zap.String("changefeed", m.cfg.ChangeFeedID))
			}
			preTs = datum.CommitTs
			preLock.Lock()
			preTsList[tsKey] = preTs
			preLock.Unlock()
		}

		key := encodeKey(module, datum.CommitTs, datum.TrackID)
		m.wb.Put(key, nil)
		size := m.cfg.DBConfig.BlockSize * writeBatchSizeFactor
		if len(m.wb.Repr()) >= size {
			m.commitData()
		}
	}
}

func (m *ModuleVerification) generatePreTsKey(module Module) string {
	return fmt.Sprintf("%s_%d", m.cfg.ChangeFeedID, module)
}

func checkOrder(module Module) bool {
	if module == Sorter || module == Sink {
		return true
	}
	return false
}

func (m *ModuleVerification) commitData() {
	m.commitMu.Lock()
	defer m.commitMu.Unlock()

	m.committing.Store(true)
	defer m.committing.Store(false)

	err := m.wb.Commit()
	if err != nil {
		log.Error(" commitData fail", zap.Error(cerror.WrapError(cerror.ErrPebbleDBError, err)))
	}
	m.wb.Reset()
}

func encodeKey(module Module, commitTs uint64, trackID []byte) []byte {
	// module, commitTs, trackID
	length := 4 + 8 + len(trackID)
	buf := make([]byte, 0, length)
	b := make([]byte, 8)

	binary.BigEndian.PutUint32(b, uint32(module))
	buf = append(buf, b[:4]...)

	binary.BigEndian.PutUint64(b, commitTs)
	buf = append(buf, b...)

	buf = append(buf, trackID...)
	return buf
}

func decodeKey(key []byte) (Module, uint64, []byte) {
	module := binary.BigEndian.Uint32(key)
	commitTs := binary.BigEndian.Uint64(key[4:])
	k := key[12:]
	return Module(module), commitTs, k
}

// Verify implement Verify api, only verify data is not lost during transfer.
// the data transfer: puller -> sorter -> (cyclic) -> sink
func (m *ModuleVerification) Verify(ctx context.Context, startTs, endTs string) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	// commit before check
	m.commitData()
	ret := m.moduleDataEqual(Puller, Sink, startTs, endTs)
	if ret {
		log.Info("module verify pass",
			zap.String("changefeed", m.cfg.ChangeFeedID),
			zap.String("startTs", startTs),
			zap.String("endTs", endTs))
		return nil
	}
	err := cerror.ErrModuleVerificationFail.FastGenByArgs(Sink.String())

	if m.cfg.CyclicEnable {
		ret = m.moduleDataEqual(Puller, Cyclic, startTs, endTs)
		if ret {
			return err
		}
		err = multierr.Append(err, cerror.ErrModuleVerificationFail.FastGenByArgs(Cyclic.String()))
	}

	ret = m.moduleDataEqual(Puller, Sorter, startTs, endTs)
	if !ret {
		err = multierr.Append(err, cerror.ErrModuleVerificationFail.FastGenByArgs(Sorter.String()))
	}

	return err
}

func (m *ModuleVerification) deleteTrackData(endTs string) error {
	m.deleteCount++
	ts, err := strconv.ParseUint(endTs, 10, 64)
	if err != nil {
		return err
	}
	// delete all data <= endTs
	end := encodeKey(Sink, ts+1, nil)
	err = m.db.DeleteRange([]byte{}, end)
	return cerror.WrapError(cerror.ErrPebbleDBError, err)
}

// GC implement GC api
func (m *ModuleVerification) GC(endTs string) error {
	err := m.deleteTrackData(endTs)
	if err != nil {
		return err
	}

	now := time.Now()
	if m.deleteCount < uint64(m.cfg.DBConfig.CompactionDeletionThreshold) && m.nextDeleteTs.After(now) {
		return nil
	}

	// cover the entire key range
	start, end := []byte{0x0}, bytes.Repeat([]byte{0xff}, 128)
	err = m.db.Compact(start, end)
	if err != nil {
		return cerror.WrapError(cerror.ErrPebbleDBError, err)
	}
	m.deleteCount = 0
	interval := time.Duration(m.cfg.DBConfig.CompactionPeriod) * time.Second
	m.nextDeleteTs = now.Add(interval)
	return nil
}

func (m *ModuleVerification) moduleDataEqual(m1, m2 Module, lower, upper string) bool {
	l, err := strconv.ParseUint(lower, 10, 64)
	if err != nil {
		return false
	}
	u, err := strconv.ParseUint(upper, 10, 64)
	if err != nil {
		return false
	}

	// include upperTs data
	u++
	l1 := encodeKey(m1, l, nil)
	u1 := encodeKey(m1, u, nil)
	iter1 := m.db.Iterator(l1, u1)
	defer func() {
		err := iter1.Release()
		if err != nil {
			log.Warn("Iterator close fail", zap.Error(err))
		}
	}()
	iter1.Seek([]byte{})

	l2 := encodeKey(m2, l, nil)
	u2 := encodeKey(m2, u, nil)
	iter2 := m.db.Iterator(l2, u2)
	defer func() {
		err := iter2.Release()
		if err != nil {
			log.Warn("Iterator close fail", zap.Error(err))
		}
	}()
	iter2.Seek([]byte{})

	for iter1.Valid() || iter2.Valid() {
		if !iter1.Valid() || !iter2.Valid() {
			log.Error("data count is not equal",
				zap.String("module1", m1.String()),
				zap.String("module2", m2.String()))
			return false
		}

		ret := keyEqual(iter1.Key(), iter2.Key())
		if ret {
			iter1.Next()
			iter2.Next()
			continue
		}
		return false
	}
	return true
}

func keyEqual(key1, key2 []byte) bool {
	m1, c1, k1 := decodeKey(key1)
	log.Info("k1", zap.String("module", m1.String()), zap.Uint64("commitTs", c1), zap.ByteString("key", k1))
	m2, c2, k2 := decodeKey(key2)
	log.Info("k2", zap.String("module", m2.String()), zap.Uint64("commitTs", c2), zap.ByteString("key", k2))
	if c1 == c2 && bytes.Compare(k1, k2) == 0 {
		return true
	}
	return false
}

// Close implement the Close api
func (m *ModuleVerification) Close() error {
	return cerror.WrapError(cerror.ErrPebbleDBError, m.db.Close())
}
