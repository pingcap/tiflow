// Copyright 2020 PingCAP, Inc.
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

package kv

import (
	"fmt"

	"github.com/pingcap/errors"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/store/driver"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/flags"
	"github.com/pingcap/tiflow/pkg/security"
	tikvconfig "github.com/tikv/client-go/v2/config"
)

// GetSnapshotMeta returns tidb meta information
func GetSnapshotMeta(tiStore tidbkv.Storage, ts uint64) *meta.Meta {
	snapshot := tiStore.GetSnapshot(tidbkv.NewVersion(ts))
	return meta.NewSnapshotMeta(snapshot)
}

// CreateTiStore creates a tikv storage client
// Note: It will return a same storage if the urls connect to a same pd cluster,
// so must be careful when you call storage.Close().
func CreateTiStore(urls string, credential *security.Credential) (tidbkv.Storage, error) {
	urlv, err := flags.NewURLsValue(urls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tiPath := fmt.Sprintf("tikv://%s?disableGC=true", urlv.HostString())
	securityCfg := tikvconfig.Security{
		ClusterSSLCA:    credential.CAPath,
		ClusterSSLCert:  credential.CertPath,
		ClusterSSLKey:   credential.KeyPath,
		ClusterVerifyCN: credential.CertAllowedCN,
	}
	d := driver.TiKVDriver{}
	// we should use OpenWithOptions to open a storage to avoid modifying tidb's GlobalConfig
	// so that we can create different storage in TiCDC by different urls and credential
	tiStore, err := d.OpenWithOptions(tiPath, driver.WithSecurity(securityCfg))
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrNewStore, err)
	}
	return tiStore, nil
}
