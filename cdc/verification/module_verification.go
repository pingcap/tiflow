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
	"github.com/pingcap/tiflow/pkg/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

//go:generate mockery --name=ModuleVerifier --inpackage
// ModuleVerifier define the interface for module verification
type ModuleVerifier interface {
	// SentTrackData sent the track data to verification
	SentTrackData(ctx context.Context, module Module, data []TrackData)
	// Verify run the module level consistency check base on the time range [startTs, endTs]
	Verify(ctx context.Context, startTs, endTs string) error
	// GC clean the checked trackData
	GC(ctx context.Context, endTs string) error
	// Close clean related resource
	Close() error
}

// Module define the module const
type Module uint32

//go:generate stringer -type=Module
const (
	Puller Module = iota
	Sorter
	Cyclic
	Sink
)

const (
	// write batch size should be larger than block size to save CPU.
	writeBatchSizeFactor = 16
)

// ModuleVerification is for module level verification
type ModuleVerification struct {
	cfg *ModuleVerificationConfig
	db  db.DB
	// write-only batch
	wb           db.Batch
	commitMu     sync.Mutex
	deleteCount  uint64
	nextDeleteTs time.Time
	etcdClient   etcd.Cli
	closed       atomic.Bool
}

// ModuleVerificationConfig is the config for ModuleVerification
type ModuleVerificationConfig struct {
	ChangefeedID        string
	DBConfig            *config.DBConfig
	MaxMemoryPercentage int
	CyclicEnable        bool
}

var (
	verifications = map[string]*ModuleVerification{}
	initLock      sync.Mutex
)

// NewModuleVerification return the module level verification per changefeed
func NewModuleVerification(ctx context.Context, cfg *ModuleVerificationConfig, etcdCli etcd.Cli) (*ModuleVerification, error) {
	if cfg == nil {
		return nil, cerror.WrapError(cerror.ErrVerificationConfigInvalid, errors.New("ModuleVerificationConfig can not be nil"))
	}
	if etcdCli == nil {
		return nil, cerror.WrapError(cerror.ErrVerificationConfigInvalid, errors.New("etcdCli can not be nil"))
	}

	initLock.Lock()
	defer initLock.Unlock()

	if v, ok := verifications[cfg.ChangefeedID]; ok {
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

	dir := filepath.Join(config.GetDefaultServerConfig().DataDir, config.DefaultVerificationDir, cfg.ChangefeedID)
	if cfg.DBConfig == nil {
		cfg.DBConfig = config.GetDefaultServerConfig().Debug.DB
	}
	pebble, err := db.OpenPebble(ctx, db.Verification, 0, dir, int(memInByte), cfg.DBConfig)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrPebbleDBError, err)
	}

	pebble.CollectMetrics(0)
	wb := pebble.Batch(0)
	m := &ModuleVerification{
		cfg:        cfg,
		db:         pebble,
		wb:         wb,
		etcdClient: etcdCli,
	}
	verifications[cfg.ChangefeedID] = m
	go m.runVerify(ctx)

	return m, nil
}

// TrackData is used to store track data transfer between cdc modules
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
	if m.closed.Load() {
		return
	}

	m.commitMu.Lock()
	defer m.commitMu.Unlock()

	for _, datum := range data {
		key := encodeKey(module, datum.CommitTs, datum.TrackID)
		m.wb.Put(key, nil)
		size := m.cfg.DBConfig.BlockSize * writeBatchSizeFactor
		if len(m.wb.Repr()) >= size {
			m.commitData()
		}
	}
}

func (m *ModuleVerification) commitData() {
	err := m.wb.Commit()
	if err != nil {
		log.Error("commitData fail", zap.Error(cerror.WrapError(cerror.ErrPebbleDBError, err)))
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

func (m *ModuleVerification) runVerify(ctx context.Context) {
	outChan := m.etcdClient.Watch(ctx, etcd.GetEtcdKeyVerification(m.cfg.ChangefeedID), "", clientv3.WithKeysOnly())
	for {
		select {
		case <-ctx.Done():
			log.Info("module runVerify ctx cancel", zap.String("changefeed", m.cfg.ChangefeedID), zap.Error(ctx.Err()))
			if err := m.Close(); err != nil {
				log.Error("module runVerify Close fail", zap.String("changefeed", m.cfg.ChangefeedID), zap.Error(err))
			}

			return
		case ret := <-outChan:
			for _, event := range ret.Events {
				log.Info("module verify event", zap.ByteString("key", event.Kv.Key), zap.ByteString("value", event.Kv.Value))
				info := &taskInfo{}
				if err := info.Unmarshal(event.Kv.Value); err != nil {
					log.Error("module verify unmarshal fail",
						zap.String("changefeed", m.cfg.ChangefeedID),
						zap.Error(err))

					continue
				}
				if !info.EnoughCheck {
					if err := m.Verify(ctx, info.StartTs, info.EndTs); err != nil {
						log.Error("module verify ret",
							zap.String("changefeed", m.cfg.ChangefeedID),
							zap.String("startTs", info.StartTs),
							zap.String("endTs", info.EndTs),
							zap.Error(err))
					}
				}
				if err := m.GC(ctx, info.EndTs); err != nil {
					log.Error("module verify gc fail",
						zap.String("changefeed", m.cfg.ChangefeedID),
						zap.String("startTs", info.StartTs),
						zap.String("endTs", info.EndTs),
						zap.Error(err))
				}
			}
		}
	}
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
	m.commitMu.Lock()
	m.commitData()
	m.commitMu.Unlock()

	ret := m.moduleDataEqual(Puller, Sink, startTs, endTs)
	if ret {
		log.Info("module verify ret pass",
			zap.String("changefeed", m.cfg.ChangefeedID),
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
	for i := Puller; i <= Sink; i++ {
		start := encodeKey(i, 0, nil)
		end := encodeKey(i, ts+1, nil)
		err = m.db.DeleteRange(start, end)
		if err != nil {
			return cerror.WrapError(cerror.ErrPebbleDBError, err)
		}
	}
	return nil
}

// GC implement GC api
func (m *ModuleVerification) GC(ctx context.Context, endTs string) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}

	if endTs == "" || m.closed.Load() {
		return nil
	}

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
	if lower == "" {
		lower = "0"
	}
	l, err := strconv.ParseUint(lower, 10, 64)
	if err != nil {
		log.Error("moduleDataEqual",
			zap.String("lower", lower),
			zap.String("m1", m1.String()),
			zap.String("m2", m2.String()),
			zap.Error(err))
		return false
	}
	u, err := strconv.ParseUint(upper, 10, 64)
	if err != nil {
		log.Error("moduleDataEqual",
			zap.String("upper", upper),
			zap.String("m1", m1.String()),
			zap.String("m2", m2.String()),
			zap.Error(err))
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
	log.Debug("moduleDataEqual", zap.String("l", lower), zap.String("u", upper), zap.Bool("iter1", iter1.Valid()))

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
	log.Debug("moduleDataEqual", zap.String("l", lower), zap.String("u", upper), zap.Bool("iter2", iter2.Valid()))

	for iter1.Valid() || iter2.Valid() {
		if !iter1.Valid() || !iter2.Valid() {
			log.Error("moduleDataEqual data count is not equal",
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
	log.Debug("keyEqual k1",
		zap.String("module", m1.String()),
		zap.Uint64("commitTs", c1),
		zap.ByteString("key1", k1))
	m2, c2, k2 := decodeKey(key2)
	log.Debug("keyEqual k2",
		zap.String("module", m2.String()),
		zap.Uint64("commitTs", c2),
		zap.ByteString("key2", k2))
	if c1 == c2 && bytes.Equal(k1, k2) {
		return true
	}
	log.Warn("keyEqual fail",
		zap.String("module1", m1.String()),
		zap.Uint64("commitTs1", c1),
		zap.ByteString("key1", k1),
		zap.String("module2", m2.String()),
		zap.Uint64("commitTs2", c2),
		zap.ByteString("key2", k2))
	return false
}

// Close implement the Close api
func (m *ModuleVerification) Close() error {
	initLock.Lock()
	defer initLock.Unlock()
	if m.closed.Load() {
		return nil
	}
	defer m.closed.Store(true)

	delete(verifications, m.cfg.ChangefeedID)
	return cerror.WrapError(cerror.ErrPebbleDBError, m.db.Close())
}
