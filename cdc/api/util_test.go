// Copyright 2021 PingCAP, Inc.
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

package api

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	mock_owner "github.com/pingcap/tiflow/cdc/owner/mock"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestRedactEtcdValuePreservesChangefeedConfiguration(t *testing.T) {
	t.Parallel()
	key := []byte("/tidb/cdc/default/default/changefeed/info/test")
	raw := []byte(`{
		"sink-uri":"mysql://user:db-secret@localhost:3306/db?time-zone=UTC&access-key=uri-secret",
		"config":{
			"memory-quota":123,
			"sink":{
				"protocol":"canal-json",
				"schema-registry":"http://user:registry-secret@localhost:8081",
				"kafka-config":{"sasl-user":"alice","sasl-password":"kafka-secret",
					"glue-schema-registry-config":{"access-key":"glue-access","secret-access-key":"glue-secret","token":"glue-token"}},
				"pulsar-config":{"authentication-token":"pulsar-secret","basic-password":"basic-secret",
					"oauth2":{"oauth2-private-key":"private-secret"}}
			},
			"consistent":{"storage":"s3://bucket/redo?access-key=redo-secret&region=us-east-1"}
		},
		"state":"normal",
		"error":{"code":"ErrSink","message":"mysql://user:error-secret@localhost/db"},
		"future-secret":"unknown-secret"
	}`)
	redacted := RedactEtcdValue(key, raw)
	for _, secret := range []string{
		"db-secret", "uri-secret", "registry-secret", "kafka-secret", "glue-access",
		"glue-secret", "glue-token", "pulsar-secret", "basic-secret", "private-secret",
		"redo-secret", "error-secret", "unknown-secret",
	} {
		require.NotContains(t, redacted, secret)
	}
	var info model.ChangeFeedInfo
	require.NoError(t, json.Unmarshal([]byte(redacted), &info))
	require.Equal(t, uint64(123), info.Config.MemoryQuota)
	require.Equal(t, "canal-json", *info.Config.Sink.Protocol)
	require.Equal(t, "alice", *info.Config.Sink.KafkaConfig.SASLUser)
	require.Contains(t, info.SinkURI, "time-zone=UTC")
	require.Contains(t, info.Config.Consistent.Storage, "region=us-east-1")
	require.Equal(t, RedactedEtcdValue, info.Error.Message)
	require.Contains(t, string(raw), "db-secret")
}

func TestRedactEtcdValueOtherMetadata(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		key, value, want string
	}{
		{"/tidb/cdc/default/default/changefeed/status/test", `{"checkpoint-ts":123}`, `"checkpoint-ts":123`},
		{"/tidb/cdc/default/default/task/position/capture/test", `{"checkpoint-ts":123,"error":{"message":"token-secret"}}`, `"checkpoint-ts":123`},
		{"/tidb/cdc/default/default/upstream/1", `{"id":1,"pd-endpoints":"http://127.0.0.1:2379"}`, `"pd-endpoints":"http://127.0.0.1:2379"`},
		{"/tidb/cdc/default/__cdc_meta__/meta/capture/capture", `{"id":"capture"}`, `"id":"capture"`},
	} {
		redacted := RedactEtcdValue([]byte(tc.key), []byte(tc.value))
		require.Contains(t, redacted, tc.want)
		require.NotContains(t, redacted, "token-secret")
	}
	for _, tc := range []struct{ key, value string }{
		{"/tidb/cdc/default/default/changefeed/info/test", `{"sink-uri":"mysql://user:secret@host/db",`},
		{"/tidb/cdc/default/default/unknown/test", `{"password":"secret"}`},
		{"/invalid", `{"password":"secret"}`},
	} {
		require.Equal(t, RedactedEtcdValue, RedactEtcdValue([]byte(tc.key), []byte(tc.value)))
	}
}

func TestIsHTTPBadRequestError(t *testing.T) {
	t.Parallel()
	err := cerror.ErrAPIInvalidParam.GenWithStack("aa")
	require.Equal(t, true, IsHTTPBadRequestError(err))
	err = cerror.ErrAPIInvalidParam.Wrap(errors.New("aa"))
	require.Equal(t, true, IsHTTPBadRequestError(err))
	err = cerror.ErrPDEtcdAPIError.GenWithStack("aa")
	require.Equal(t, false, IsHTTPBadRequestError(err))
	err = nil
	require.Equal(t, false, IsHTTPBadRequestError(err))
}

func TestCollectTaskStatuses(t *testing.T) {
	t.Parallel()
	changefeedID := model.DefaultChangeFeedID("test-cf")

	// success: task statuses are collected per capture.
	ctrl := gomock.NewController(t)
	provider := mock_owner.NewMockStatusProvider(ctrl)
	provider.EXPECT().GetAllTaskStatuses(gomock.Any(), changefeedID).Return(
		map[model.CaptureID]*model.TaskStatus{
			"capture-1": {Tables: map[model.TableID]*model.TableReplicaInfo{
				3508: {}, 3520: {},
			}},
		}, nil)
	taskStatus, err := CollectTaskStatuses(context.Background(), provider, changefeedID)
	require.NoError(t, err)
	require.Len(t, taskStatus, 1)
	require.Equal(t, "capture-1", taskStatus[0].CaptureID)
	require.ElementsMatch(t, []model.TableID{3508, 3520}, taskStatus[0].Tables)

	// a lookup failure is propagated to the caller, which decides whether the
	// task_status can be omitted.
	provider = mock_owner.NewMockStatusProvider(ctrl)
	provider.EXPECT().GetAllTaskStatuses(gomock.Any(), changefeedID).Return(
		nil, cerror.ErrChangeFeedNotExists.GenWithStackByArgs(changefeedID))
	taskStatus, err = CollectTaskStatuses(context.Background(), provider, changefeedID)
	require.Error(t, err)
	require.Nil(t, taskStatus)
}
