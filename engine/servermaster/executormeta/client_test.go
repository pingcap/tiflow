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

package executormeta

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	pkgModel "github.com/pingcap/tiflow/engine/pkg/orm/model"
	"github.com/pingcap/tiflow/engine/servermaster/executormeta/model"
	"github.com/pingcap/tiflow/pkg/label"
	"github.com/stretchr/testify/require"
)

func TestExecutorClient(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err)

	mock.ExpectQuery("SELECT VERSION()").
		WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("5.7.35-log"))

	db, err := pkgOrm.NewGormDB(sqlDB, metaModel.StoreTypeMySQL)
	require.NoError(t, err)
	client := newClientImpl(db)

	now := time.Now()
	createdAt := now.Add(time.Second)
	updatedAt := createdAt.Add(time.Second)

	executor := &model.Executor{
		Model: pkgModel.Model{
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
		},
		ID:         "executor-0-1234",
		Name:       "executor-0",
		Address:    "127.0.0.1:1234",
		Capability: 20,
		Labels: map[label.Key]label.Value{
			"key1": "val1",
			"key2": "val2",
		},
	}

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO `executors` (`created_at`,`updated_at`,`id`,`name`,`address`,`capability`,`labels`) VALUES (?,?,?,?,?,?,?)")).
		WithArgs(createdAt, updatedAt, executor.ID, executor.Name, executor.Address, executor.Capability, "{\"key1\":\"val1\",\"key2\":\"val2\"}").
		WillReturnResult(sqlmock.NewResult(1, 1))
	err = client.CreateExecutor(context.Background(), executor)
	require.NoError(t, err)

	mock.ExpectExec(regexp.QuoteMeta("UPDATE `executors` SET `address`=?,`capability`=?,`id`=?,`labels`=?,`name`=?,`updated_at`=? WHERE id = ?")).
		WithArgs(executor.Address, executor.Capability, executor.ID, "{\"key1\":\"val1\",\"key2\":\"val2\"}", executor.Name, sqlmock.AnyArg(), executor.ID).
		WillReturnResult(sqlmock.NewResult(0, 1))
	err = client.UpdateExecutor(context.Background(), executor)
	require.NoError(t, err)

	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM `executors` WHERE id = ?")).
		WithArgs(executor.ID).WillReturnResult(sqlmock.NewResult(0, 1))
	err = client.DeleteExecutor(context.Background(), executor.ID)
	require.NoError(t, err)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM `executors`")).
		WillReturnRows(sqlmock.NewRows([]string{
			"seq_id", "created_at", "updated_at", "id", "name", "address", "capability", "labels",
		}).AddRow(1, createdAt, updatedAt, executor.ID, executor.Name,
			executor.Address, executor.Capability, "{\"key1\":\"val1\",\"key2\":\"val2\"}"))
	executors, err := client.QueryExecutors(context.Background())
	require.NoError(t, err)
	require.Equal(t, executor, executors[0])
}
