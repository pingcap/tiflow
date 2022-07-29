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

package internal

import (
	"testing"

	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

type duplicatedClientBuilder struct{}

func (b *duplicatedClientBuilder) ClientType() metaModel.ClientType {
	return metaModel.EtcdKVClientType
}

func (b *duplicatedClientBuilder) NewKVClientWithNamespace(cc metaModel.ClientConn,
	projectID metaModel.ProjectID, jobID metaModel.JobID,
) (metaModel.KVClient, error) {
	return nil, nil
}

func TestMustRegisterClientBuilder(t *testing.T) {
	t.Parallel()

	r := GlobalClientBuilderRegistra()
	require.Len(t, r.reg, 3)

	defer func() {
		err := recover()
		require.NotNil(t, err)
		require.Regexp(t, "client type is already existed", err.(string))
	}()

	MustRegisterClientBuilder(&duplicatedClientBuilder{})
}

func TestGetClientBuilder(t *testing.T) {
	t.Parallel()

	_, err := GetClientBuilder(metaModel.MockKVClientType)
	require.NoError(t, err)

	_, err = GetClientBuilder(metaModel.EtcdKVClientType)
	require.NoError(t, err)

	_, err = GetClientBuilder(metaModel.SQLKVClientType)
	require.NoError(t, err)
}
