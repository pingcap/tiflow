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
	"os"
	"path/filepath"
	"strings"
	"time"

	cerror "github.com/pingcap/ticdc/pkg/errors"
)

// GetTimezone returns the timezone specified by the name
func GetTimezone(name string) (tz *time.Location, err error) {
	switch strings.ToLower(name) {
	case "", "system", "local":
		tz, err = GetLocalTimezone()
		err = cerror.WrapError(cerror.ErrLoadTimezone, err)
	default:
		tz, err = time.LoadLocation(name)
		err = cerror.WrapError(cerror.ErrLoadTimezone, err)
	}
	return
}

func getTimezoneFromZonefile(zonefile string) (tz *time.Location, err error) {
	// the linked path of `/etc/localtime` sample:
	// MacOS: /var/db/timezone/zoneinfo/Asia/Shanghai
	// Linux: /usr/share/zoneinfo/Asia/Shanghai
	region := filepath.Base(filepath.Dir(zonefile))
	zone := filepath.Base(zonefile)
	var tzName string
	if region == "zoneinfo" {
		tzName = zone
	} else {
		tzName = filepath.Join(region, zone)
	}
	tz, err = time.LoadLocation(tzName)
	err = cerror.WrapError(cerror.ErrLoadTimezone, err)
	return
}

// GetLocalTimezone returns the timezone in local system
func GetLocalTimezone() (*time.Location, error) {
	if time.Local.String() != "Local" {
		return time.Local, nil
	}
	str, err := os.Readlink("/etc/localtime")
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrLoadTimezone, err)
	}
	return getTimezoneFromZonefile(str)
}
