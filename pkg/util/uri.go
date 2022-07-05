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

package util

import (
	"net"
	"strings"
)

// IsValidIPv6AddressInURI reports whether hostPort is a valid IPv6 address in URI.
// See: https://www.ietf.org/rfc/rfc2732.txt.
func IsValidIPv6AddressInURI(hostPort string) bool {
	hostname := hostPort

	colon := strings.LastIndexByte(hostname, ':')
	if colon != -1 && validOptionalPort(hostname[colon:]) {
		hostname = hostname[:colon]
	}

	isIPv6 := IsIPv6Address(hostname)
	if !isIPv6 {
		// IPv6 address must have `:`.
		return false
	}

	if !strings.HasPrefix(hostname, "[") || !strings.HasSuffix(hostname, "]") {
		return false
	}

	return true
}

// IsIPv6Address reports whether hostname is a IPv6 address.
// Notice: There is hostname not host(host+port).
func IsIPv6Address(hostname string) bool {
	ip := net.ParseIP(hostname)
	if ip == nil {
		return false
	}
	return strings.Contains(hostname, ":")
}

// validOptionalPort reports whether port is either an empty string
// or matches /^:\d*$/
func validOptionalPort(port string) bool {
	if port == "" {
		return true
	}
	if port[0] != ':' {
		return false
	}
	for _, b := range port[1:] {
		if b < '0' || b > '9' {
			return false
		}
	}
	return true
}
