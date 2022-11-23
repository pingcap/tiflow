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

package errors

import (
	"regexp"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

const (
	dmErrBaseFormat = "\\[code=([0-9]+):class=[a-zA-Z-]+:scope=[a-zA-Z-]+:level=[a-zA-Z]+\\](.*)"
)

var dmErrRegexp = regexp.MustCompile(dmErrBaseFormat)

// ToDMError tries best to construct a DM terror from an error object
// - If err wraps a DM standard error, return it directly.
// - If not, checks whether error message matches DM standard error format
//   - If it does, construct a DM terror from the error code and message.
//   - Otherwise returns the original error
func ToDMError(err error) error {
	if _, ok := errors.Cause(err).(*terror.Error); ok {
		return err
	}

	subMatch := dmErrRegexp.FindStringSubmatch(err.Error())
	if len(subMatch) > 2 {
		code, _ := strconv.Atoi(subMatch[1])
		if baseError, ok := terror.ErrorFromCode(terror.ErrCode(code)); ok {
			return baseError.Generatef(subMatch[2])
		}
	}

	return err
}
