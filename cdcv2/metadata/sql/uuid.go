// Copyright 2023 PingCAP, Inc.
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

package sql

import (
	"context"
	"hash"
	"hash/crc64"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tiflow/cdcv2/metadata"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type uuidGenerator interface {
	GenChangefeedUUID(ctx context.Context) (metadata.ChangefeedUUID, error)
}

// NewUUIDGenerator creates a new UUID generator.
func NewUUIDGenerator(config string, db *gorm.DB) uuidGenerator {
	switch config {
	case "mock":
		return newMockUUIDGenerator()
	case "random-crc64":
		hasher := crc64.New(crc64.MakeTable(crc64.ISO))
		return newRandomUUIDGenerator(hasher)
	case "random-fnv64":
		return newRandomUUIDGenerator(fnv.New64())
	default:
		return newORMUUIDGenerator(taskCDCChangefeedUUID, db)
	}
}

type mockUUIDGenerator struct {
	epoch atomic.Uint64
}

func newMockUUIDGenerator() uuidGenerator {
	return &mockUUIDGenerator{}
}

// GenChangefeedUUID implements uuidGenerator interface.
func (g *mockUUIDGenerator) GenChangefeedUUID(ctx context.Context) (metadata.ChangefeedUUID, error) {
	return g.epoch.Add(1), nil
}

type randomUUIDGenerator struct {
	uuidGen uuid.Generator
	hasher  hash.Hash64
}

func newRandomUUIDGenerator(hasher hash.Hash64) uuidGenerator {
	return &randomUUIDGenerator{
		uuidGen: uuid.NewGenerator(),
		hasher:  hasher,
	}
}

// GenChangefeedUUID implements uuidGenerator interface.
func (g *randomUUIDGenerator) GenChangefeedUUID(ctx context.Context) (metadata.ChangefeedUUID, error) {
	g.hasher.Reset()
	g.hasher.Write([]byte(g.uuidGen.NewString()))
	return g.hasher.Sum64(), nil
}

// TODO: implement sql based UUID generator.

const (
	tableNameLogicEpoch   = "logic_epoch"
	taskCDCChangefeedUUID = "cdc-changefeed-uuid"
)

type logicEpochDO struct {
	TaskID string `gorm:"column:task_id;type:varchar(128); primaryKey" json:"task_id"`
	Epoch  uint64 `gorm:"column:epoch;type:bigint(20) unsigned;not null" json:"epoch"`

	CreatedAt time.Time `json:"created-at"`
	UpdatedAt time.Time `json:"updated-at"`
}

func (l *logicEpochDO) TableName() string {
	return tableNameLogicEpoch
}

type ormUUIDGenerator struct {
	db     *gorm.DB
	taskID string

	once sync.Once
}

func newORMUUIDGenerator(taskID string, db *gorm.DB) uuidGenerator {
	return &ormUUIDGenerator{
		db:     db,
		taskID: taskID,
	}
}

func (g *ormUUIDGenerator) initlize(ctx context.Context) error {
	if err := g.db.AutoMigrate(&logicEpochDO{}); err != nil {
		return errors.WrapError(errors.ErrMetaOpFailed, err, "ormUUIDGeneratorInitlize")
	}
	// Do nothing on conflict
	if err := g.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).
		Create(&logicEpochDO{
			TaskID: g.taskID,
			Epoch:  0,
		}).Error; err != nil {
		return errors.WrapError(errors.ErrMetaOpFailed, err, "ormUUIDGeneratorInitlize")
	}
	return nil
}

func (g *ormUUIDGenerator) GenChangefeedUUID(ctx context.Context) (metadata.ChangefeedUUID, error) {
	var err error
	g.once.Do(func() {
		err = g.initlize(ctx)
	})
	if err != nil {
		return 0, errors.Trace(err)
	}

	uuidDO := &logicEpochDO{}
	// every job owns its logic epoch
	err = g.db.WithContext(ctx).
		Where("task_id = ?", g.taskID).
		Transaction(func(tx *gorm.DB) error {
			if err := tx.Model(uuidDO).
				Update("epoch", gorm.Expr("epoch + ?", 1)).Error; err != nil {
				// return any error will rollback
				return errors.WrapError(errors.ErrMetaOpFailed, err, "GenChangefeedUUID")
			}

			if err := tx.Select("epoch").Limit(1).Find(uuidDO).Error; err != nil {
				return errors.WrapError(errors.ErrMetaOpFailed, err, "GenChangefeedUUID")
			}
			return nil
		})
	if err != nil {
		return 0, errors.Trace(err)
	}
	return uuidDO.Epoch, nil
}
