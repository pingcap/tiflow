package util

import (
	"os"
	"path"
	"strings"
	"time"
)

func GetTimezone(name string) (*time.Location, error) {
	switch strings.ToLower(name) {
	case "", "system", "local":
		return GetLocalTimezone()
	default:
		return time.LoadLocation(name)
	}
}

func GetLocalTimezone() (*time.Location, error) {
	if time.Local.String() != "Local" {
		return time.Local, nil
	}
	str, err := os.Readlink("/etc/localtime")
	if err != nil {
		return nil, err
	}
	tzName := path.Join(path.Base(path.Dir(str)), path.Base(str))
	return time.LoadLocation(tzName)
}
