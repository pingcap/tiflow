// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"encoding/json"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestRedactEtcdValue(t *testing.T) {
	t.Parallel()
	password := "replica-config-secret"
	replica := config.GetDefaultReplicaConfig()
	replica.Sink.KafkaConfig = &config.KafkaConfig{SASLPassword: &password}
	info := &model.ChangeFeedInfo{
		ID: "orders", Namespace: "team-a",
		SinkURI: "mysql://user:dsn-secret@localhost/db?token=uri-secret",
		Config:  replica,
	}
	value, err := json.Marshal(info)
	require.NoError(t, err)
	original := string(value)
	for _, namespace := range []string{"default", "team-a"} {
		key := []byte("/tidb/cdc/default/" + namespace + "/changefeed/info/orders")
		masked := RedactEtcdValue(key, value)
		for _, secret := range []string{"dsn-secret", "uri-secret", password} {
			require.NotContains(t, masked, secret)
		}
		var result model.ChangeFeedInfo
		require.NoError(t, json.Unmarshal([]byte(masked), &result))
		require.Equal(t, info.ID, result.ID)
		require.Contains(t, result.SinkURI, "xxxxx")
		require.Equal(t, original, string(value), "redaction must not change etcd data")
	}
	key := []byte("/tidb/cdc/default/team-a/changefeed/info/orders")
	masked := RedactEtcdValue(key, []byte(`{"sink-uri":"mysql://user:malformed-secret@host"`))
	require.NotContains(t, masked, "malformed-secret")
	require.Contains(t, masked, "redacted")
	require.Equal(t, "42", RedactEtcdValue([]byte("/tidb/cdc/default/__cdc_meta__/meta/meta-version"), []byte("42")))
}
