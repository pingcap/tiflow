//  Copyright 2021 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

package common

import (
	"fmt"
	"strings"

	"github.com/pingcap/tiflow/cdc/model"
)

// FilterChangefeedFiles return the files that match to the changefeed.
func FilterChangefeedFiles(files []string, changefeedID model.ChangeFeedID) []string {
	var (
		matcher string
		res     []string
	)

	if changefeedID.Namespace == "default" {
		matcher = fmt.Sprintf("_%s_", changefeedID.ID)
	} else {
		matcher = fmt.Sprintf("_%s_%s_", changefeedID.Namespace, changefeedID.ID)
	}
	for _, file := range files {
		if strings.Contains(file, matcher) {
			res = append(res, file)
		}
	}
	return res
}
