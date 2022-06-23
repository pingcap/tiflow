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

package v2

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

type MockUpstream struct {
	ID             uint64
	PDAddrs        []string `json:"pd_addrs"`
	SecurityConfig *security.Credential
}

type mockUpstreamManager struct {
	mockUpstream       MockUpstream
	getDefaultUpstream func() *MockUpstream
}

func (up *mockUpstreamManager) GetDefaultUpstream() *MockUpstream {
	return &up.mockUpstream
}

type configToSend struct {
	Namespace string `json:"namespace"`
	ID        string `json:"changefeed_id"`
	StartTs   uint64 `json:"start_ts"`
	TargetTs  uint64 `json:"target_ts"`
	SinkURI   string `json:"sink_uri"`
	Engine    string `json:"engine"`

	ReplicaConfig *ReplicaConfig `json:"replica_config"`

	SyncPointEnabled  bool          `json:"sync_point_enabled"`
	SyncPointInterval time.Duration `json:"sync_point_interval"`

	PDAddrs       []string `json:"pd_addrs"`
	CAPath        string   `json:"ca_path"`
	CertPath      string   `json:"cert_path"`
	KeyPath       string   `json:"key_path"`
	CertAllowedCN []string `json:"cert_allowed_cn"`
}

const (
	changeFeedID4Test1 = "changefeedID-for-test-1"
	blackHoleSink      = "blackhole://"
)

type mockPDClient4CfTest struct {
	pd.Client
}

func (c *mockPDClient4CfTest) Close() {}

func TestCreateChangefeed(t *testing.T) {
	t.Parallel()
	NewOpenAPIV2()

	// test succeed
	config1 := struct {
		ID      string `json:"changefeed_id"`
		SinkURI string `json:"sink_uri"`
		PDAddrs string `json:"pd_addrs"`
	}{changeFeedID4Test1, blackHoleSink, ""}
	b, err := json.Marshal(&config1)
	require.Nil(t, err)
	body := bytes.NewReader(b)

	getPdCliFunc1 := func(a, b, c any) (mockPDClient4CfTest, error) {
		return mockPDClient4CfTest{}, nil
	}
	getPdCliFunc2 := func(a, b, c any) (mockPDClient4CfTest, error) {
		return , nil
	}

}

func TestUpdateChangefeed(t *testing.T) {}
