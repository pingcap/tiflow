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

package util

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/ticdc/pkg/httputil"
	"github.com/pingcap/ticdc/pkg/security"
	"go.uber.org/zap"
)

// minPDVersion is the version of the minimal compatible PD.
var minPDVersion *semver.Version = semver.New("4.0.0-rc.1")

// MinTiKVVersion is the version of the minimal compatible TiKV.
var MinTiKVVersion *semver.Version = semver.New("4.0.0-rc.1")

var versionHash = regexp.MustCompile("-[0-9]+-g[0-9a-f]{7,}")

// ErrVersionIncompatible is an error for running CDC on an incompatible Cluster.
var ErrVersionIncompatible = errors.NewNoStackError("version is incompatible")

func removeVAndHash(v string) string {
	if v == "" {
		return v
	}
	v = versionHash.ReplaceAllLiteralString(v, "")
	v = strings.TrimSuffix(v, "-dirty")
	return strings.TrimPrefix(v, "v")
}

// CheckClusterVersion check TiKV and PD version.
func CheckClusterVersion(
	ctx context.Context, client pd.Client, pdHTTP string, credential *security.Credential, errorTiKVIncompat bool,
) error {
	err := CheckStoreVersion(ctx, client, 0 /* check all TiKV */)
	if err != nil {
		if errorTiKVIncompat {
			return err
		}
		log.Warn("check TiKV version failed", zap.Error(err))
	}
	httpCli, err := httputil.NewClient(credential)
	if err != nil {
		return errors.Annotate(err, "fail to validate TLS settings")
	}
	// See more: https://github.com/pingcap/pd/blob/v4.0.0-rc.1/server/api/version.go
	pdVer := struct {
		Version string `json:"version"`
	}{}
	req, err := http.NewRequestWithContext(
		ctx, http.MethodGet, fmt.Sprintf("%s/pd/api/v1/version", pdHTTP), nil)
	if err != nil {
		return errors.Annotate(err, "fail to request PD")
	}
	resp, err := httpCli.Do(req)
	if err != nil {
		return errors.Annotate(err, "fail to request PD")
	}
	if resp.StatusCode < 200 && resp.StatusCode >= 300 {
		return errors.BadRequestf("fail to requet PD %s", resp.Status)
	}
	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Annotate(err, "fail to request PD")
	}
	err = json.Unmarshal(content, &pdVer)
	if err != nil {
		return errors.Annotate(err, "fail to request PD")
	}
	err = resp.Body.Close()
	if err != nil {
		return errors.Annotate(err, "fail to request PD")
	}
	ver, err := semver.NewVersion(removeVAndHash(pdVer.Version))
	if err != nil {
		return err
	}
	ord := ver.Compare(*minPDVersion)
	if ord < 0 {
		return errors.Annotatef(ErrVersionIncompatible, "PD %s is not supported, require minimal version %s",
			removeVAndHash(pdVer.Version), minPDVersion)
	}
	return nil
}

// CheckStoreVersion checks whether the given TiKV is compatible with this CDC.
// If storeID is 0, it checks all TiKV.
func CheckStoreVersion(ctx context.Context, client pd.Client, storeID uint64) error {
	var stores []*metapb.Store
	var err error
	if storeID == 0 {
		stores, err = client.GetAllStores(ctx, pd.WithExcludeTombstone())
	} else {
		stores = make([]*metapb.Store, 1)
		stores[0], err = client.GetStore(ctx, storeID)
	}
	if err != nil {
		return err
	}

	for _, s := range stores {
		ver, err := semver.NewVersion(removeVAndHash(s.Version))
		if err != nil {
			return err
		}
		ord := ver.Compare(*MinTiKVVersion)
		if ord < 0 {
			return errors.Annotatef(ErrVersionIncompatible, "TiKV %s is not supported, require minimal version %s",
				removeVAndHash(s.Version), MinTiKVVersion)
		}
	}
	return nil
}
