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

package election_test

import (
	"context"
	"encoding/json"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tiflow/engine/pkg/election"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func newSQLStorageAndMock(t *testing.T) (*election.SQLStorage, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	mock.ExpectExec(regexp.QuoteMeta("CREATE TABLE IF NOT EXISTS leader_election (id int NOT NULL, version bigint NOT NULL, record text NOT NULL, PRIMARY KEY (id))")).
		WillReturnResult(sqlmock.NewResult(0, 0))

	s, err := election.NewSQLStorage(db, "leader_election")
	require.NoError(t, err)

	return s, mock
}

func TestSQLStorageGetEmptyRecord(t *testing.T) {
	t.Parallel()

	s, mock := newSQLStorageAndMock(t)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT version, record FROM leader_election WHERE id = ?")).
		WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"version", "record"}))
	record, err := s.Get(context.Background())
	require.NoError(t, err)
	require.Equal(t, &election.Record{}, record)
}

func TestSQLStorageGetExistingRecord(t *testing.T) {
	t.Parallel()

	s, mock := newSQLStorageAndMock(t)

	expectedRecord := &election.Record{
		LeaderID: "id1",
		Members: []*election.Member{
			{
				ID:   "id1",
				Name: "name1",
			},
			{
				ID:   "id2",
				Name: "name2",
			},
		},
		Version: 1,
	}
	recordBytes, err := json.Marshal(expectedRecord)
	require.NoError(t, err)

	mock.ExpectQuery(regexp.QuoteMeta("SELECT version, record FROM leader_election WHERE id = ?")).
		WithArgs(1).WillReturnRows(sqlmock.NewRows([]string{"version", "record"}).AddRow(1, recordBytes))
	record, err := s.Get(context.Background())
	require.NoError(t, err)
	require.Equal(t, expectedRecord, record)
}

func TestSQLStorageInsertRecord(t *testing.T) {
	t.Parallel()

	s, mock := newSQLStorageAndMock(t)

	record := &election.Record{
		LeaderID: "id1",
		Members: []*election.Member{
			{
				ID:   "id1",
				Name: "name1",
			},
		},
		Version: 0, // 0 means record not created before.
	}
	recordBytes, err := json.Marshal(record)
	require.NoError(t, err)

	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO leader_election (id, version, record) VALUES (?, ?, ?)")).
		WithArgs(1, int64(1), recordBytes).WillReturnResult(sqlmock.NewResult(1, 1))

	err = s.Update(context.Background(), record)
	require.NoError(t, err)
}

func TestSQLStorageUpdateRecord(t *testing.T) {
	t.Parallel()

	s, mock := newSQLStorageAndMock(t)

	record := &election.Record{
		LeaderID: "id1",
		Members: []*election.Member{
			{
				ID:   "id1",
				Name: "name1",
			},
		},
		Version: 1,
	}
	recordBytes, err := json.Marshal(record)
	require.NoError(t, err)

	mock.ExpectExec(regexp.QuoteMeta("UPDATE leader_election SET version = ?, record = ? WHERE id = ? AND version = ?")).
		WithArgs(int64(2), recordBytes, 1, int64(1)).WillReturnResult(sqlmock.NewResult(0, 0))
	err = s.Update(context.Background(), record)
	require.True(t, errors.Is(err, errors.ErrElectionRecordConflict))

	mock.ExpectExec(regexp.QuoteMeta("UPDATE leader_election SET version = ?, record = ? WHERE id = ? AND version = ?")).
		WithArgs(int64(2), recordBytes, 1, int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
	err = s.Update(context.Background(), record)
	require.NoError(t, err)
}
