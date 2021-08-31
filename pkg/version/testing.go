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

package version

import (
	"context"
	"fmt"
	"net/http"

	"github.com/pingcap/kvproto/pkg/metapb"
	pd "github.com/tikv/pd/client"
)

// MockPDClient mocks a pd client for tests.
type MockPDClient struct {
	pd.Client
	GetAllStoresFunc  func() []*metapb.Store
	GetVersionFunc    func() string
	GetStatusCodeFunc func() int
}

// GetAllStores impls pd.Client.GetAllStores
func (m *MockPDClient) GetAllStores(ctx context.Context, opts ...pd.GetStoreOption) ([]*metapb.Store, error) {
	if m.GetAllStoresFunc != nil {
		return m.GetAllStoresFunc(), nil
	}
	return []*metapb.Store{}, nil
}

func (m *MockPDClient) ServeHTTP(resp http.ResponseWriter, _ *http.Request) {
	// set status code at first, else will not work
	if m.GetStatusCodeFunc != nil {
		resp.WriteHeader(m.GetStatusCodeFunc())
	}

	if m.GetVersionFunc != nil {
		_, _ = resp.Write([]byte(fmt.Sprintf(`{"version":"%s"}`, m.GetVersionFunc())))
	}
}
