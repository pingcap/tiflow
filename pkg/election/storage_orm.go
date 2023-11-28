// Copyright 2022 PingCAP, Inc.
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

package election

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"

	"github.com/pingcap/log"
	ormUtil "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/pkg/errors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	recordRowID = 1
	// leaderRowID is not used for node probing, it is only used to lock the leader row.
	leaderRowID = 2
)

// Value implements the driver.Valuer interface
func (r Record) Value() (driver.Value, error) {
	return json.Marshal(r)
}

// Scan implements the sql.Scanner interface
func (r *Record) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, r)
}

// TableNameElection is the table name of election.
var TableNameElection = "election"

// DO mapped from table <election>
type DO struct {
	ID       uint32  `gorm:"column:id;type:int(10) unsigned;primaryKey" json:"id"`
	LeaderID string  `gorm:"column:leader_id;type:text;not null" json:"leader_id"`
	Record   *Record `gorm:"column:record;type:text" json:"record"`
	Version  uint64  `gorm:"column:version;type:bigint(20) unsigned;not null" json:"version"`
}

// TableName Election's table name
func (*DO) TableName() string {
	return TableNameElection
}

// ORMStorage is a storage implementation based on SQL database.
type ORMStorage struct {
	db        *gorm.DB
	tableName string
}

// NewORMStorageFromSQLDB creates a new ORMStorage from sql.DB.
func NewORMStorageFromSQLDB(backendDB *sql.DB, tableName string) (*ORMStorage, error) {
	db, err := ormUtil.NewGormDB(backendDB, "mysql")
	if err != nil {
		return nil, err
	}
	return NewORMStorage(db, tableName)
}

// NewORMStorage creates a new ORMStorage.
func NewORMStorage(db *gorm.DB, tableName string) (*ORMStorage, error) {
	TableNameElection = tableName
	if err := db.AutoMigrate(&DO{}); err != nil {
		return nil, errors.Trace(err)
	}

	return &ORMStorage{
		db:        db,
		tableName: tableName,
	}, nil
}

// Get implements Storage.Get.
func (s *ORMStorage) Get(ctx context.Context) (*Record, error) {
	var do DO

	ret := s.db.WithContext(ctx).Where("id = ?", recordRowID).Limit(1).Find(&do)
	if ret.Error != nil {
		if ret.Error == gorm.ErrRecordNotFound {
			return &Record{}, nil
		}
		return nil, errors.Trace(ret.Error)
	}
	if ret.RowsAffected == 0 {
		return &Record{}, nil
	}

	do.Record.Version = int64(do.Version)
	return do.Record, nil
}

// Update implements Storage.Update.
func (s *ORMStorage) Update(ctx context.Context, record *Record, isLeaderChanged bool) error {
	if record.Version == 0 {
		if !isLeaderChanged {
			log.Panic("invalid operation")
		}
		return s.create(ctx, record)
	}
	return s.update(ctx, record, isLeaderChanged)
}

func (s *ORMStorage) update(ctx context.Context, record *Record, isLeaderChanged bool) error {
	handleRet := func(ret *gorm.DB) error {
		if ret.Error != nil {
			return errors.Trace(ret.Error)
		}
		if ret.RowsAffected != 1 {
			return errors.ErrElectionRecordConflict.GenWithStackByArgs()
		}
		return nil
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// TODO: find more efficient way
		ret := tx.Where("id = ? AND version = ?", recordRowID, record.Version).
			Updates(DO{
				LeaderID: record.LeaderID,
				Record:   record,
				Version:  uint64(record.Version) + 1,
			})
		if err := handleRet(ret); err != nil {
			return errors.Trace(err)
		}

		if isLeaderChanged {
			ret := tx.Where("id = ?", leaderRowID).
				Updates(DO{
					LeaderID: record.LeaderID,
					Record:   nil,
					Version:  uint64(record.Version) + 1,
				})
			return handleRet(ret)
		}
		return nil
	})
}

func (s *ORMStorage) create(ctx context.Context, record *Record) error {
	rows := []*DO{
		{
			ID:       recordRowID,
			LeaderID: record.LeaderID,
			Record:   record,
			Version:  uint64(record.Version) + 1,
		},
		{
			ID:       leaderRowID,
			LeaderID: record.LeaderID,
			Record:   nil,                        /* record is not saved in leader row */
			Version:  uint64(record.Version) + 1, /* equals to recordRow */
		},
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		ret := tx.WithContext(ctx).Create(rows)
		if ret.Error != nil {
			return errors.Trace(ret.Error)
		}

		if ret.RowsAffected == 0 {
			return errors.ErrElectionRecordConflict.GenWithStackByArgs()
		} else if ret.RowsAffected != int64(len(rows)) {
			log.Panic("Transaction atomicity is broken when updating election record")
		}
		return nil
	})
}

// TxnWithLeaderLock execute a transaction with leader row locked.
func (s *ORMStorage) TxnWithLeaderLock(ctx context.Context, leaderID string, fc func(tx *gorm.DB) error) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var do DO
		ret := tx.Select("leader_id").Where("id = ? and leader_id = ?", leaderRowID, leaderID).
			Clauses(clause.Locking{
				Strength: "SHARE",
				Table:    clause.Table{Name: clause.CurrentTable},
			}).Limit(1).Find(&do)
		if ret.Error != nil {
			if ret.Error == gorm.ErrRecordNotFound {
				return errors.ErrElectorNotLeader.GenWithStackByArgs(leaderID)
			}
			return errors.Trace(ret.Error)
		}
		if ret.RowsAffected != 1 {
			return errors.ErrElectorNotLeader.GenWithStackByArgs(leaderID)
		}
		return fc(tx)
	})
}
