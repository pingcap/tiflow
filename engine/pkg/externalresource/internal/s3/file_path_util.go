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

package s3

import (
	"fmt"
	"strings"
)

type pathPredFunc func(path string) bool

func getPathPredAlwaysTrue() pathPredFunc {
	return func(_ string) bool {
		return true
	}
}

func getPathPredByName(target string) pathPredFunc {
	return func(path string) bool {
		resName, _, ok := strings.Cut(path, "/")
		if !ok {
			return false
		}
		return resName == target
	}
}

func getPathPredByPersistedResources(
	resources persistedResources, prefixCnt int,
) pathPredFunc {
	return func(path string) bool {
		resName := ""
		for i := 0; i < prefixCnt; i++ {
			prefix, newPath, ok := strings.Cut(path, "/")
			if !ok {
				return false
			}

			if resName == "" {
				resName = prefix
			} else {
				resName = fmt.Sprintf("%s/%s", resName, prefix)
			}
			path = newPath
		}
		_, ok := resources[resName]
		return !ok
	}
}
