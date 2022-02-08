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

package flags

import (
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/types"
)

// URLsValue define a slice of URLs as a type
type URLsValue types.URLs

// Set parses a command line set of URLs formatted like:
// http://127.0.0.1:2380,http://10.1.1.2:80
func (us *URLsValue) Set(s string) error {
	strs := strings.Split(s, ",")
	nus, err := types.NewURLs(strs)
	if err != nil {
		return errors.Trace(err)
	}

	*us = URLsValue(nus)
	return nil
}

// HostString return a string of host:port format list separated by comma
func (us *URLsValue) HostString() string {
	all := make([]string, len(*us))
	for i, u := range *us {
		all[i] = u.Host
	}
	return strings.Join(all, ",")
}

// NewURLsValue return a URLsValue from a string of URLs list
func NewURLsValue(init string) (*URLsValue, error) {
	v := &URLsValue{}
	err := v.Set(init)
	return v, err
}
