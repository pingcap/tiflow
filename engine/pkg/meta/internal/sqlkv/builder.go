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

package sqlkv

import (
	"database/sql"

	"github.com/pingcap/tiflow/engine/pkg/meta/internal/sqlkv/namespace"
	"github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
)

// ClientBuilderImpl is the implement of ClientBuilder for sqlkv
type ClientBuilderImpl struct{}

// ClientType implements ClientType of clientBuilder
func (b *ClientBuilderImpl) ClientType() model.ClientType {
	return model.SQLKVClientType
}

// NewKVClientWithNamespace implements NewKVClientWithNamespace of clientBuilder
func (b *ClientBuilderImpl) NewKVClientWithNamespace(cc model.ClientConn,
	projectID model.ProjectID, jobID model.JobID,
) (model.KVClient, error) {
	conn, err := cc.GetConn()
	if err != nil {
		return nil, err
	}

	db, ok := conn.(*sql.DB)
	if !ok {
		return nil, errors.ErrMetaParamsInvalid.GenWithStack(
			"invalid ClientConn type for sql kvclient builder, conn type:%s", cc.StoreType())
	}

	tableName := namespace.TableNameWithNamespace(projectID)
	return NewSQLKVClientImpl(db, cc.StoreType(), tableName, jobID)
}
