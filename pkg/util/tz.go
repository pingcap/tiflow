package util

import (
	"os"
	"path"
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

// GetLocalTimezone returns the timezone in local system
func GetLocalTimezone() (*time.Location, error) {
	if time.Local.String() != "Local" {
		return time.Local, nil
	}
	str, err := os.Readlink("/etc/localtime")
	if err != nil {
		return nil, err
	}
	// the linked path of `/etc/localtime`
	// MacOS: /var/db/timezone/zoneinfo/Asia/Shanghai
	// Linux: /etc/usr/share/zoneinfo/Asia/Shanghai
	tzName := path.Join(path.Base(path.Dir(str)), path.Base(str))
	return time.LoadLocation(tzName)
}
