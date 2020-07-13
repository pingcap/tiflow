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
)

// GetTimezone returns the timezone specified by the name
func GetTimezone(name string) (*time.Location, error) {
	switch strings.ToLower(name) {
	case "", "system", "local":
		return GetLocalTimezone()
	default:
		return time.LoadLocation(name)
	}
}

func getTimezoneFromZonefile(zonefile string) (*time.Location, error) {
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
	return time.LoadLocation(tzName)
}

// GetLocalTimezone returns the timezone in local system
func GetLocalTimezone() (*time.Location, error) {
	if time.Local.String() != "Local" {
		return time.Local, nil
	}
	str, err := os.Readlink("/etc/localtime")
	if err != nil {
		return nil, err
	}
	return getTimezoneFromZonefile(str)
}
