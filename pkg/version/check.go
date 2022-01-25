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
	"net/http"
	"regexp"
	"strings"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/httputil"
	"github.com/pingcap/tiflow/pkg/security"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

var (
	// minPDVersion is the version of the minimal compatible PD.
	minPDVersion *semver.Version = semver.New("5.1.0-alpha")
	// maxPDVersion is the version of the maximum compatible PD.
	// Compatible versions are in [minPDVersion, maxPDVersion)
	// 9999.0.0 disables the check effectively in the master branch.
	maxPDVersion *semver.Version = semver.New("9999.0.0")

	// MinTiKVVersion is the version of the minimal compatible TiKV.
	MinTiKVVersion *semver.Version = semver.New("5.1.0-alpha")
	// maxTiKVVersion is the version of the maximum compatible TiKV.
	// Compatible versions are in [MinTiKVVersion, maxTiKVVersion)
	// 9999.0.0 disables the check effectively in the master branch.
	maxTiKVVersion *semver.Version = semver.New("9999.0.0")

	// MinTiCDCVersion is the version of the minimal compatible TiCDC.
	MinTiCDCVersion *semver.Version = semver.New("5.1.0-alpha")
	// Compatible versions are in [MinTiCDCVersion, MaxTiCDCVersion)
	// 9999.0.0 disables the check effectively in the master branch.
	maxTiCDCVersion *semver.Version = semver.New("9999.0.0")

	// CaptureInfo.Version is added since v4.0.11,
	// we use the minimal release version as default.
	defaultTiCDCVersion *semver.Version = semver.New("4.0.1")
)

var versionHash = regexp.MustCompile("-[0-9]+-g[0-9a-f]{7,}(-dev)?")

func removeVAndHash(v string) string {
	if v == "" {
		return v
	}
	v = versionHash.ReplaceAllLiteralString(v, "")
	v = strings.TrimSuffix(v, "-dirty")
	return strings.TrimPrefix(v, "v")
}

// CheckClusterVersion check TiKV and PD version.
// need only one PD alive and match the cdc version.
func CheckClusterVersion(
	ctx context.Context, client pd.Client, pdAddrs []string, credential *security.Credential, errorTiKVIncompat bool) error {
	err := CheckStoreVersion(ctx, client, 0 /* check all TiKV */)
	if err != nil {
		if errorTiKVIncompat {
			return err
		}
		log.Warn("check TiKV version failed", zap.Error(err))
	}

	for _, pdAddr := range pdAddrs {
		err = CheckPDVersion(ctx, pdAddr, credential)
		if err == nil {
			break
		}
	}

	return err
}

// CheckPDVersion check PD version.
func CheckPDVersion(ctx context.Context, pdAddr string, credential *security.Credential) error {
	// See more: https://github.com/pingcap/pd/blob/v4.0.0-rc.1/server/api/version.go
	pdVer := struct {
		Version string `json:"version"`
	}{}

	httpClient, err := httputil.NewClient(credential)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(
		ctx, http.MethodGet, fmt.Sprintf("%s/pd/api/v1/version", pdAddr), nil)
	if err != nil {
		return cerror.ErrCheckClusterVersionFromPD.GenWithStackByArgs(err)
	}

	resp, err := httpClient.Do(req)
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

	ver, err := semver.NewVersion(removeVAndHash(pdVer.Version))
	if err != nil {
		err = errors.Annotate(err, "invalid PD version")
		return cerror.WrapError(cerror.ErrNewSemVersion, err)
	}

	minOrd := ver.Compare(*minPDVersion)
	if minOrd < 0 {
		arg := fmt.Sprintf("PD %s is not supported, the minimal compatible version is %s",
			removeVAndHash(pdVer.Version), minPDVersion)
		return cerror.ErrVersionIncompatible.GenWithStackByArgs(arg)
	}
	maxOrd := ver.Compare(*maxPDVersion)
	if maxOrd >= 0 {
		arg := fmt.Sprintf("PD %s is not supported, the maximum compatible version is %s",
			removeVAndHash(pdVer.Version), maxPDVersion)
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
		ver, err := semver.NewVersion(removeVAndHash(s.Version))
		if err != nil {
			err = errors.Annotate(err, "invalid TiKV version")
			return cerror.WrapError(cerror.ErrNewSemVersion, err)
		}
		minOrd := ver.Compare(*MinTiKVVersion)
		if minOrd < 0 {
			arg := fmt.Sprintf("TiKV %s is not supported, the minimal compatible version is %s",
				removeVAndHash(s.Version), MinTiKVVersion)
			return cerror.ErrVersionIncompatible.GenWithStackByArgs(arg)
		}
		maxOrd := ver.Compare(*maxTiKVVersion)
		if maxOrd >= 0 {
			arg := fmt.Sprintf("TiKV %s is not supported, the maximum compatible version is %s",
				removeVAndHash(s.Version), maxTiKVVersion)
			return cerror.ErrVersionIncompatible.GenWithStackByArgs(arg)
		}
	}
	return nil
}

// TiCDCClusterVersion is the version of TiCDC cluster
type TiCDCClusterVersion struct {
	*semver.Version
}

// IsUnknown returns whether this is an unknown version
func (v *TiCDCClusterVersion) IsUnknown() bool {
	return v.Version == nil
}

// ShouldEnableOldValueByDefault returns whether old value should be enabled by default
func (v *TiCDCClusterVersion) ShouldEnableOldValueByDefault() bool {
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

// ShouldRunCliWithAPIClientByDefault returns whether to run cmd cli with api client by default
func (v *TiCDCClusterVersion) ShouldRunCliWithAPIClientByDefault() bool {
	// we assume the unknown version to be the latest version
	if v.Version == nil {
		return true
	}

	return !v.LessThan(*semver.New("5.4.0")) || (v.Major == 5 && v.Minor == 4 && v.Patch == 0)
}

// TiCDCClusterVersionUnknown is a read-only variable to represent the unknown cluster version
var TiCDCClusterVersionUnknown = TiCDCClusterVersion{}

// GetTiCDCClusterVersion returns the version of ticdc cluster
func GetTiCDCClusterVersion(captureVersion []string) (TiCDCClusterVersion, error) {
	if len(captureVersion) == 0 {
		return TiCDCClusterVersionUnknown, nil
	}
	var minVer *semver.Version
	for _, versionStr := range captureVersion {
		var ver *semver.Version
		var err error
		if versionStr != "" {
			ver, err = semver.NewVersion(removeVAndHash(versionStr))
		} else {
			ver = defaultTiCDCVersion
		}
		if err != nil {
			err = errors.Annotate(err, "invalid CDC cluster version")
			return TiCDCClusterVersionUnknown, cerror.WrapError(cerror.ErrNewSemVersion, err)
		}
		if minVer == nil || ver.Compare(*minVer) < 0 {
			minVer = ver
		}
	}
	return TiCDCClusterVersion{minVer}, nil
}

// CheckTiCDCClusterVersion returns the version of ticdc cluster
func CheckTiCDCClusterVersion(cdcClusterVer TiCDCClusterVersion) (unknown bool, err error) {
	if cdcClusterVer.Version == nil {
		return true, nil
	}
	ver := cdcClusterVer.Version
	minOrd := ver.Compare(*MinTiCDCVersion)
	if minOrd < 0 {
		arg := fmt.Sprintf("TiCDC %s is not supported, the minimal compatible version is %s"+
			"try tiup ctl:%s cdc [COMMAND]",
			ver, MinTiCDCVersion, ver)
		return false, cerror.ErrVersionIncompatible.GenWithStackByArgs(arg)
	}
	maxOrd := ver.Compare(*maxTiCDCVersion)
	if maxOrd >= 0 {
		arg := fmt.Sprintf("TiCDC %s is not supported, the maximum compatible version is %s"+
			"try tiup ctl:%s cdc [COMMAND]",
			ver, maxTiCDCVersion, ver)
		return false, cerror.ErrVersionIncompatible.GenWithStackByArgs(arg)
	}
	return false, nil
}
