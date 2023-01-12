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

package client

import (
	"os"
	"testing"
	"time"

	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"
)

func TestNewExecutorClientNotBlocking(t *testing.T) {
	t.Parallel()

	factory := newExecutorClientFactory(nil, nil)

	startTime := time.Now()
	_, _ = factory.NewExecutorClient("invalid:1234")
	require.Less(t, time.Since(startTime), 1*time.Second)
}

func TestExecutorClientFactoryIllegalCredentials(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	caPath := dir + "/ca.pem"
	err := os.WriteFile(caPath, []byte("invalid ca pem"), 0o600)
	require.NoError(t, err)

	credentials := &security.Credential{
		CAPath: caPath, // illegal CA path to trigger an error
	}
	factory := newExecutorClientFactory(credentials, nil)
	_, err = factory.NewExecutorClient("127.0.0.1:1234")
	require.Error(t, err)
}
