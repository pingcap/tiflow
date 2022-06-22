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
	"testing"

	"github.com/pingcap/tiflow/pkg/security"
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

func TestCreateChangefeed(t *testing.T) {}

func TestUpdateChangefeed(t *testing.T) {}
