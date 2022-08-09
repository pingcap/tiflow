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
	"regexp"
	"testing"

	metaMock "github.com/pingcap/tiflow/engine/pkg/meta/mock"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

const (
	fakeProjectID = "fakeProject"
	fakeJobID     = "fakeJob"
)

type errorClientConn struct{}

func (c *errorClientConn) Initialize(conf *metaModel.StoreConfig) error {
	return nil
}

func (c *errorClientConn) StoreType() metaModel.StoreType {
	return metaModel.StoreType("unknown")
}

func (c *errorClientConn) GetConn() (interface{}, error) {
	return nil, nil
}

func (c *errorClientConn) Close() error {
	return nil
}

func TestNewKVClientWithNamespace(t *testing.T) {
	t.Parallel()

	cc := metaMock.NewMockClientConn()
	require.NotNil(t, cc)
	cli, err := NewKVClientWithNamespace(cc, fakeProjectID, fakeJobID)
	require.NoError(t, err)
	require.NotNil(t, cli)

	_, err = NewKVClientWithNamespace(&errorClientConn{}, fakeProjectID, fakeJobID)
	require.Error(t, err)
	require.Regexp(t, regexp.QuoteMeta("[DFLOW:ErrMetaClientTypeNotSupport]meta client type not support:unknown-kvclient"),
		err.Error())
}
