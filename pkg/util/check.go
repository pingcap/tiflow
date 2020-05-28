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
	pd "github.com/pingcap/pd/v4/client"
)

var minPDVersion *semver.Version = semver.New("4.0.0-rc.1")
var minTiKVVersion *semver.Version = semver.New("4.0.0-rc.1")

func removeV(v string) string {
	if v == "" {
		return v
	}
	return strings.TrimPrefix(v, "v")
}

// CheckClusterVersion check TiKV and PD version.
func CheckClusterVersion(ctx context.Context, client pd.Client, pdHTTP string) error {
	stores, err := client.GetAllStores(ctx, pd.WithExcludeTombstone())
	if err != nil {
		return err
	}
	for _, s := range stores {
		ver, err := semver.NewVersion(removeV(s.Version))
		if err != nil {
			return err
		}
		ord := ver.Compare(*minTiKVVersion)
		if ord < 0 {
			return errors.NotSupportedf("TiKV %s is not supported, require minimal version %s",
				removeV(s.Version), minTiKVVersion)
		}
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
	resp, err := http.DefaultClient.Do(req)
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
	ver, err := semver.NewVersion(removeV(pdVer.Version))
	if err != nil {
		return err
	}
	ord := ver.Compare(*minPDVersion)
	if ord < 0 {
		return errors.NotSupportedf("PD %s is not supported, require minimal version %s",
			removeV(pdVer.Version), minPDVersion)
	}
	return nil
}

// IsValidUUIDv4 returns true if the uuid is a valid uuid
func IsValidUUIDv4(uuid string) bool {
	if len(uuid) != 36 {
		return false
	}
	match, _ := regexp.Match("[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}", []byte(uuid))
	return match
}
