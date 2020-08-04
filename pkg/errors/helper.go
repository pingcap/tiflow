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

package errors

import (
	"github.com/pingcap/errors"
)

// WrapError generates a new error based on given `*errors.Error`, wraps the err
// as parameter via the `GenWithStackByArgs`
// The message format of `*errors.Error` must be `format message: %s`
// In this way the Error() string of returned error will keep the RFC format
// In the future if the error lib provides a native error wrap way, we could just
// change this function to the new API.
func WrapError(rfcError *errors.Error, err error) error {
	if err == nil {
		return nil
	}
	return rfcError.GenWithStackByArgs(err)
}
