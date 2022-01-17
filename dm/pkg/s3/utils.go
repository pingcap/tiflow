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

package s3

import (
	"net/url"
	"strings"

	"github.com/pingcap/errors"
)

// AdjustS3Path adjust s3 rawURL, add uniqueId into s3 path.
func AdjustS3Path(rawURL string, uniqueID string) (bool, string, error) {
	if rawURL == "" {
		return false, "", nil
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return false, "", errors.Trace(err)
	}
	if u.Scheme == "s3" {
		if uniqueID != "" {
			u.Path = strings.TrimRight(u.Path, "/") + "." + uniqueID
			return true, u.String(), nil
		}
		return true, rawURL, nil
	}
	return false, rawURL, nil
}
