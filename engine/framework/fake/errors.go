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

package fake

import (
	"fmt"
	"regexp"

	"github.com/pingcap/tiflow/pkg/errors"
)

// JobUnRetryableError is used in fake job and unit test only
type JobUnRetryableError struct {
	errIn error
}

// NewJobUnRetryableError creates a new JobUnRetryableError
func NewJobUnRetryableError(errIn error) *JobUnRetryableError {
	return &JobUnRetryableError{
		errIn: errIn,
	}
}

// Error implements error interface
func (e *JobUnRetryableError) Error() string {
	return fmt.Sprintf("%s: %s", e.message(), e.errIn)
}

func (e *JobUnRetryableError) message() string {
	return "fake job unretryable error"
}

const fakeJobErrorFormat = "fake job unretryable error: (.*)"

var fakeJobErrorRegexp = regexp.MustCompile(fakeJobErrorFormat)

// ToFakeJobError tries best to construct a fake job error from an error object
func ToFakeJobError(err error) error {
	var errOut *JobUnRetryableError
	if errors.As(err, &errOut) {
		return err
	}

	subMatch := fakeJobErrorRegexp.FindStringSubmatch(err.Error())
	if len(subMatch) > 1 {
		return NewJobUnRetryableError(errors.New(subMatch[1]))
	}

	return err
}
