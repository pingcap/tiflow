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
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/engine"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/httputil"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/security"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

var (
	// minPDVersion is the version of the minimal compatible PD.
	minPDVersion = semver.New("5.1.0-alpha")
	// maxPDVersion is the version of the maximum compatible PD.
	// Compatible versions are in [minPDVersion, maxPDVersion)
	maxPDVersion = semver.New("8.0.0")

	// MinTiKVVersion is the version of the minimal compatible TiKV.
	MinTiKVVersion = semver.New("5.1.0-alpha")
	// maxTiKVVersion is the version of the maximum compatible TiKV.
	// Compatible versions are in [MinTiKVVersion, maxTiKVVersion)
	maxTiKVVersion = semver.New("8.0.0")

	// CaptureInfo.Version is added since v4.0.11,
	// we use the minimal release version as default.
	defaultTiCDCVersion = semver.New("4.0.1")

	// MinTiCDCVersion is the version of the minimal allowed TiCDC version.
	MinTiCDCVersion = semver.New("6.3.0-alpha")
	// MaxTiCDCVersion is the version of the maximum allowed TiCDC version.
	// for version `x.y.z`, max allowed `x+2.0.0`
	MaxTiCDCVersion = semver.New("8.0.0-alpha")
)

var versionHash = regexp.MustCompile("-[0-9]+-g[0-9a-f]{7,}(-dev)?")

// SanitizeVersion remove the prefix "v" and suffix git hash.
func SanitizeVersion(v string) string {
	if v == "" {
		return v
	}
	v = versionHash.ReplaceAllLiteralString(v, "")
	v = strings.TrimSuffix(v, "-dirty")
	return strings.TrimPrefix(v, "v")
}

var checkClusterVersionRetryTimes = 10

// CheckClusterVersion check TiKV and PD version.
// need only one PD alive and match the cdc version.
func CheckClusterVersion(
	ctx context.Context, client pd.Client, pdAddrs []string,
	credential *security.Credential, errorTiKVIncompat bool,
) error {
	err := CheckStoreVersion(ctx, client, 0 /* check all TiKV */)
	if err != nil {
		if errorTiKVIncompat {
			return err
		}
		log.Warn("check TiKV version failed", zap.Error(err))
	}

	for _, pdAddr := range pdAddrs {
		// check pd version with retry, if the pdAddr is a service or lb address
		// the http client may connect to an unhealthy PD that returns 503
		err = retry.Do(ctx, func() error {
			return checkPDVersion(ctx, pdAddr, credential)
		}, retry.WithBackoffBaseDelay(time.Millisecond.Milliseconds()*10),
			retry.WithBackoffMaxDelay(time.Second.Milliseconds()),
			retry.WithMaxTries(uint64(checkClusterVersionRetryTimes)),
			retry.WithIsRetryableErr(cerror.IsRetryableError))
		if err == nil {
			break
		}
	}

	return err
}

// CheckTiCDCVersion return true if all cdc instance have valid semantic version
// the whole cluster only allow at most 2 different version instances
// and should in the range [MinTiCDCVersion, MaxTiCDCVersion)
func CheckTiCDCVersion(versions map[string]struct{}) error {
	if len(versions) <= 1 {
		return nil
	}
	if len(versions) >= 3 {
		arg := fmt.Sprintf("all running cdc instance belong to %d different versions, "+
			"it's not allowed", len(versions))
		return cerror.ErrVersionIncompatible.GenWithStackByArgs(arg)
	}

	ver := &semver.Version{}
	for v := range versions {
		if err := ver.Set(SanitizeVersion(v)); err != nil {
			return cerror.WrapError(cerror.ErrNewSemVersion, err)
		}
		if ver.Compare(*MinTiCDCVersion) < 0 {
			arg := fmt.Sprintf("TiCDC %s is not supported, the minimal compatible version is %s",
				SanitizeVersion(v), MinTiCDCVersion)
			return cerror.ErrVersionIncompatible.GenWithStackByArgs(arg)
		}
		if ver.Compare(*MaxTiCDCVersion) >= 0 {
			arg := fmt.Sprintf("TiCDC %s is not supported, only support version less than %s",
				SanitizeVersion(v), MaxTiCDCVersion)
			return cerror.ErrVersionIncompatible.GenWithStackByArgs(arg)
		}
	}
	return nil
}

// checkPDVersion check PD version.
func checkPDVersion(ctx context.Context, pdAddr string, credential *security.Credential) error {
	// See more: https://github.com/pingcap/pd/blob/v4.0.0-rc.1/server/api/version.go
	pdVer := struct {
		Version string `json:"version"`
	}{}

	httpClient, err := httputil.NewClient(credential)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := httpClient.Get(ctx, fmt.Sprintf("%s/pd/api/v1/version", pdAddr))
	if err != nil {
		return cerror.ErrCheckClusterVersionFromPD.GenWithStackByArgs(err)
	}
	defer resp.Body.Close()

	content, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var arg string
		if err != nil {
			arg = fmt.Sprintf("%s %s %s", resp.Status, content, err)
		} else {
			arg = fmt.Sprintf("%s %s", resp.Status, content)
		}
		return cerror.ErrCheckClusterVersionFromPD.GenWithStackByArgs(arg)
	}

	err = json.Unmarshal(content, &pdVer)
	if err != nil {
		return cerror.ErrCheckClusterVersionFromPD.GenWithStackByArgs(err)
	}

	ver, err := semver.NewVersion(SanitizeVersion(pdVer.Version))
	if err != nil {
		err = errors.Annotate(err, "invalid PD version")
		return cerror.WrapError(cerror.ErrNewSemVersion, err)
	}

	minOrd := ver.Compare(*minPDVersion)
	if minOrd < 0 {
		arg := fmt.Sprintf("PD %s is not supported, the minimal compatible version is %s",
			SanitizeVersion(pdVer.Version), minPDVersion)
		return cerror.ErrVersionIncompatible.GenWithStackByArgs(arg)
	}
	maxOrd := ver.Compare(*maxPDVersion)
	if maxOrd >= 0 {
		arg := fmt.Sprintf("PD %s is not supported, only support version less than %s",
			SanitizeVersion(pdVer.Version), maxPDVersion)
		return cerror.ErrVersionIncompatible.GenWithStackByArgs(arg)
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
		return cerror.WrapError(cerror.ErrGetAllStoresFailed, err)
	}

	for _, s := range stores {
		if engine.IsTiFlash(s) {
			continue
		}

		ver, err := semver.NewVersion(SanitizeVersion(s.Version))
		if err != nil {
			err = errors.Annotate(err, "invalid TiKV version")
			return cerror.WrapError(cerror.ErrNewSemVersion, err)
		}
		minOrd := ver.Compare(*MinTiKVVersion)
		if minOrd < 0 {
			arg := fmt.Sprintf("TiKV %s is not supported, the minimal compatible version is %s",
				SanitizeVersion(s.Version), MinTiKVVersion)
			return cerror.ErrVersionIncompatible.GenWithStackByArgs(arg)
		}
		maxOrd := ver.Compare(*maxTiKVVersion)
		if maxOrd >= 0 {
			arg := fmt.Sprintf("TiKV %s is not supported, only support version less than %s",
				SanitizeVersion(s.Version), maxTiKVVersion)
			return cerror.ErrVersionIncompatible.GenWithStackByArgs(arg)
		}
	}
	return nil
}

// TiCDCClusterVersion is the version of TiCDC cluster
type TiCDCClusterVersion struct {
	*semver.Version
}

// LessThan500RC returns true if th cluster version is less than 5.0.0-rc
func (v *TiCDCClusterVersion) LessThan500RC() bool {
	// we assume the unknown version to be the latest version
	return v.Version == nil || !v.LessThan(*semver.New("5.0.0-rc"))
}

// ShouldEnableUnifiedSorterByDefault returns whether Unified Sorter should be enabled by default
func (v *TiCDCClusterVersion) ShouldEnableUnifiedSorterByDefault() bool {
	if v.Version == nil {
		// we assume the unknown version to be the latest version
		return true
	}
	// x >= 4.0.13 AND x != 5.0.0-rc
	if v.String() == "5.0.0-rc" {
		return false
	}
	return !v.LessThan(*semver.New("4.0.13")) || (v.Major == 4 && v.Minor == 0 && v.Patch == 13)
}

// ShouldRunCliWithOpenAPI returns whether to run cmd cli with open api
func (v *TiCDCClusterVersion) ShouldRunCliWithOpenAPI() bool {
	// we assume the unknown version to be the latest version
	if v.Version == nil {
		return true
	}

	return !v.LessThan(*semver.New("6.2.0")) || (v.Major == 6 && v.Minor == 2 && v.Patch == 0)
}

// ticdcClusterVersionUnknown is a read-only variable to represent the unknown cluster version
var ticdcClusterVersionUnknown = TiCDCClusterVersion{}

// GetTiCDCClusterVersion returns the version of ticdc cluster
func GetTiCDCClusterVersion(captureVersion []string) (TiCDCClusterVersion, error) {
	if len(captureVersion) == 0 {
		return ticdcClusterVersionUnknown, nil
	}
	var minVer *semver.Version
	for _, versionStr := range captureVersion {
		var ver *semver.Version
		var err error
		if versionStr != "" {
			ver, err = semver.NewVersion(SanitizeVersion(versionStr))
		} else {
			ver = defaultTiCDCVersion
		}
		if err != nil {
			err = errors.Annotate(err, "invalid CDC cluster version")
			return ticdcClusterVersionUnknown, cerror.WrapError(cerror.ErrNewSemVersion, err)
		}
		if minVer == nil || ver.Compare(*minVer) < 0 {
			minVer = ver
		}
	}
	return TiCDCClusterVersion{minVer}, nil
}
