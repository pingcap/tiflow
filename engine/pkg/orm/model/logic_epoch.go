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

package model

import (
	"context"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/atomic"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	defaultMinEpoch = 1
)

// LogicEpoch is used to generate increasing epoch
// We use union columns <JobID, Epoch> as uk to achieve job-level isolation
type LogicEpoch struct {
	Model
	JobID string `gorm:"type:varchar(128) not null;uniqueIndex:uidx_jk"`
	Epoch int64  `gorm:"type:bigint not null default 1"`
}

// TODO: after we split the orm model, move this client out of the file

// EpochClient defines the client to generate epoch
type EpochClient interface {
	// GenEpoch increases the backend epoch by 1 and return the new epoch
	// Guarantee to be thread-safe
	GenEpoch(ctx context.Context) (int64, error)

	// Close releases some inner resources
	Close() error
}

// NewEpochClient news a EpochClient
// Make Sure to call 'InitEpochModel' to create backend table before
// calling 'NewEpochClient'
func NewEpochClient(jobID string, db *gorm.DB) (*epochClient, error) {
	if db == nil {
		return nil, errors.ErrMetaParamsInvalid.GenWithStackByArgs("input db is nil")
	}

	return &epochClient{
		jobID: jobID,
		db:    db,
	}, nil
}

type epochClient struct {
	// isInitialized is for lazy initialization
	isInitialized atomic.Bool
	jobID         string
	db            *gorm.DB
}

func (e *epochClient) initialize(ctx context.Context) error {
	// Do nothing on conflict
	if err := e.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).
		Create(&LogicEpoch{
			JobID: e.jobID,
			Epoch: defaultMinEpoch,
		}).Error; err != nil {
		return errors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

// GenEpoch implements GenEpoch of EpochClient
// Guarantee to be thread-safe
func (e *epochClient) GenEpoch(ctx context.Context) (int64, error) {
	// we make lazy initialization for two reasons:
	// 1. Not all kinds of client need calling GenEpoch
	// 2. Some components depend on framework meta client before initializing the backend meta table
	if !e.isInitialized.Load() {
		if err := e.initialize(ctx); err != nil {
			return int64(0), err
		}
		e.isInitialized.Store(true)
	}

	failpoint.InjectContext(ctx, "genEpochDelay", nil)
	if e.db == nil {
		return int64(0), errors.ErrMetaParamsInvalid.GenWithStackByArgs("inner db is nil")
	}

	var epoch int64
	// every job owns its logic epoch
	err := e.db.WithContext(ctx).
		Where("job_id = ?", e.jobID).
		Transaction(func(tx *gorm.DB) error {
			//(1)update epoch = epoch + 1
			if err := tx.Model(&LogicEpoch{}).
				Update("epoch", gorm.Expr("epoch + ?", 1)).Error; err != nil {
				// return any error will rollback
				return err
			}

			//(2)select epoch
			var logicEp LogicEpoch
			if err := tx.First(&logicEp).Error; err != nil {
				return err
			}
			epoch = logicEp.Epoch

			// return nil will commit the whole transaction
			return nil
		})
	if err != nil {
		return int64(0), err
	}

	return epoch, nil
}

// Close implements Close of EpochClient
func (e *epochClient) Close() error {
	return nil
}
