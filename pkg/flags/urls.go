// Copyright 2019 PingCAP, Inc.
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
	"net"
	"net/url"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/types"
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

func (us *URLsValue) String() string {
	all := make([]string, len(*us))
	for i, u := range *us {
		all[i] = u.String()
	}
	return strings.Join(all, ",")
}

// HostString return a string of host:port format list separated by comma
func (us *URLsValue) HostString() string {
	all := make([]string, len(*us))
	for i, u := range *us {
		all[i] = u.Host
	}
	return strings.Join(all, ",")
}

// StringSlice return a slice of string with formatted URL
func (us *URLsValue) StringSlice() []string {
	all := make([]string, len(*us))
	for i, u := range *us {
		all[i] = u.String()
	}
	return all
}

// URLSlice return a slice of URLs
func (us *URLsValue) URLSlice() []url.URL {
	urls := []url.URL(*us)
	return urls
}

// NewURLsValue return a URLsValue from a string of URLs list
func NewURLsValue(init string) (*URLsValue, error) {
	v := &URLsValue{}
	err := v.Set(init)
	return v, err
}

// ParseHostPortAddr returns a scheme://host:port or host:port list
func ParseHostPortAddr(s string) ([]string, error) {
	strs := strings.Split(s, ",")
	addrs := make([]string, 0, len(strs))

	for _, str := range strs {
		str = strings.TrimSpace(str)

		// str may looks like 127.0.0.1:8000
		if _, _, err := net.SplitHostPort(str); err == nil {
			addrs = append(addrs, str)
			continue
		}

		u, err := url.Parse(str)
		if err != nil {
			return nil, errors.Errorf("parse url %s failed %v", str, err)
		}
		if u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "unix" && u.Scheme != "unixs" {
			return nil, errors.Errorf("URL scheme must be http, https, unix, or unixs: %s", str)
		}
		if _, _, err := net.SplitHostPort(u.Host); err != nil {
			return nil, errors.Errorf(`URL address does not have the form "host:port": %s`, str)
		}
		if u.Path != "" {
			return nil, errors.Errorf("URL must not contain a path: %s", str)
		}
		addrs = append(addrs, u.String())
	}

	return addrs, nil
}
