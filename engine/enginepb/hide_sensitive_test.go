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

package enginepb

import (
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

var (
	expectOutput = `:\\\"password: \\\\\\\"******\\\\\\\"\\\\nssl-ca-bytes: \\\"******\\\"\\\\nssl-key-bytes: \\\"******\\\"\\\\nssl-cert-bytes: \\\"******\\\"`
	config       = `password: "random-password"
ssl-ca-bytes:  -----BEGIN CERTIFICATE-----
random1
random1-random2
random1-random2-random3
-----END CERTIFICATE-----
ssl-key-bytes: '-----BEGIN PRIVATE KEY-----
random1
random1-random2-random3
random1-random2
-----END PRIVATE KEY-----'
ssl-cert-bytes:  "-----BEGIN CERTIFICATE REQUEST-----
random1-random2-random3
random1-random2
random1
-----END CERTIFICATE REQUEST-----"`
)

func TestJobConfig(t *testing.T) {
	job := &Job{
		Id:     "test_job",
		Config: []byte(config),
	}
	c := &CreateJobRequest{
		TenantId:  "test_tenant",
		ProjectId: "test_project",
		Job:       job,
	}

	var buffer zaptest.Buffer
	err := logutil.InitLogger(&logutil.Config{Level: "info"}, logutil.WithOutputWriteSyncer(&buffer))
	require.NoError(t, err)

	log.Info("test CreateJobRequest", zap.Any("c", c))
	require.Contains(t, buffer.String(), expectOutput)
	buffer.Reset()

	log.Info("test Job", zap.Any("job", job))
	require.Contains(t, buffer.String(), expectOutput)
}

func TestConfig(t *testing.T) {
	configs := []zapcore.ObjectMarshaler{
		&PreDispatchTaskRequest{TaskConfig: []byte(config)},
		&QueryMetaStoreResponse{Config: []byte(config)},
		&QueryStorageConfigResponse{Config: []byte(config)},
	}

	var buffer zaptest.Buffer
	err := logutil.InitLogger(&logutil.Config{Level: "info"}, logutil.WithOutputWriteSyncer(&buffer))
	require.NoError(t, err)

	for _, c := range configs {
		log.Info("test config", zap.Any("c", c))
		require.Contains(t, buffer.String(), expectOutput)
		buffer.Reset()
	}
}
