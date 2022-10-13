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

package upgrade

import (
	"encoding/json"

	"github.com/pingcap/tiflow/dm/common"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/pkg/version"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	// The current internal version number of the DM cluster used when upgrading from an older version.
	// NOTE: +1 when a new incompatible version is introduced, so it's different from the release version.
	// NOTE: it's the version of the cluster (= the version of DM-master leader now), other component versions are not recorded yet.
	currentInternalNo uint64 = 4
	// The minimum internal version number of the DM cluster used when importing from v1.0.x.
	minInternalNo uint64 = 0
)

var (
	// CurrentVersion represents the current version of the cluster.
	CurrentVersion = NewVersion(currentInternalNo, version.ReleaseVersion)
	// MinVersion represents the minimum version of the cluster.
	// this version only be set after finished importing from v1.0.x, but has not upgraded to the current version.
	MinVersion = NewVersion(minInternalNo, "min-ver")
)

// Version represents the version of the DM cluster used when upgrading from an older version.
type Version struct {
	InternalNo uint64 `json:"internal-no"` // internal version number
	ReleaseVer string `json:"release-ver"` // release version, like `v2.0.0`, human readable
}

// NewVersion creates a new instance of Version.
func NewVersion(internalNo uint64, releaseVer string) Version {
	return Version{
		InternalNo: internalNo,
		ReleaseVer: releaseVer,
	}
}

// Compare compares the version with another version.
// NOTE: we only compare `InternalNo` now.
func (v Version) Compare(other Version) int {
	if v.InternalNo < other.InternalNo {
		return -1
	} else if v.InternalNo > other.InternalNo {
		return 1
	}
	return 0
}

// NotSet returns whether the version is not set.
func (v Version) NotSet() bool {
	return v.InternalNo == 0 && v.ReleaseVer == ""
}

// String implements Stringer interface.
func (v Version) String() string {
	s, _ := v.toJSON()
	return s
}

// toJSON returns the string of JSON represent.
func (v Version) toJSON() (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// versionFromJSON constructs version from its JSON represent.
func versionFromJSON(s string) (v Version, err error) {
	err = json.Unmarshal([]byte(s), &v)
	return
}

// PutVersion puts the version into etcd.
func PutVersion(cli *clientv3.Client, ver Version) (int64, error) {
	value, err := ver.toJSON()
	if err != nil {
		return 0, err
	}
	op := clientv3.OpPut(common.ClusterVersionKey, value)
	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(op))
	return rev, err
}

// GetVersion gets the version from etcd.
func GetVersion(cli *clientv3.Client) (Version, int64, error) {
	txnResp, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(clientv3.OpGet(common.ClusterVersionKey)))
	if err != nil {
		return Version{}, 0, err
	}
	verResp := txnResp.Responses[0].GetResponseRange()
	if verResp.Count == 0 {
		return Version{}, rev, nil
	} else if verResp.Count > 1 {
		// this should not happen
		return Version{}, rev, terror.ErrUpgradeVersionEtcdFail.Generatef("too many versions (%d) exist", verResp.Count)
	}

	ver, err := versionFromJSON(string(verResp.Kvs[0].Value))
	if err != nil {
		return Version{}, rev, err
	}
	return ver, rev, nil
}
