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

package meta

import (
	"github.com/pingcap/tiflow/engine/pkg/meta/internal/etcdkv"
	"github.com/pingcap/tiflow/engine/pkg/meta/internal/sqlkv"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
)

// NewClientConn new a client connection
func NewClientConn(storeConf *metaModel.StoreConfig) (metaModel.ClientConn, error) {
	switch storeConf.StoreType {
	case metaModel.StoreTypeEtcd:
		return etcdkv.NewClientConnImpl(storeConf)
	case metaModel.StoreTypeMySQL:
		return sqlkv.NewClientConnImpl(storeConf)
	}

	return nil, errors.ErrMetaClientTypeNotSupport.
		GenWithStackByArgs(metaModel.ToClientType(storeConf.StoreType))
}
