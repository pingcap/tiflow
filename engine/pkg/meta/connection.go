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
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
)

// NewClientConn new a client connection
func NewClientConn(storeConf *metaModel.StoreConfig) (metaModel.ClientConn, error) {
	var cc metaModel.ClientConn

	switch storeConf.StoreType {
	case metaModel.StoreTypeEtcd:
		cc = etcdkv.NewClientConnImpl()
	default:
		return nil, cerrors.ErrMetaClientTypeNotSupport.
			GenWithStackByArgs(metaModel.ToClientType(storeConf.StoreType))
	}

	if err := cc.Initialize(storeConf); err != nil {
		return nil, err
	}

	return cc, nil
}
