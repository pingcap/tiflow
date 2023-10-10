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
	"net/url"
	"strings"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// IsValidIPv6AddressFormatInURI reports whether hostPort is a valid IPv6 address in URI.
// See: https://www.ietf.org/rfc/rfc2732.txt.
func IsValidIPv6AddressFormatInURI(hostPort string) bool {
	hostname := hostPort

	colon := strings.LastIndexByte(hostname, ':')
	if colon != -1 && validOptionalPort(hostname[colon:]) {
		hostname = hostname[:colon]
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

// MaskSinkURI returns a sink uri that sensitive infos has been masked.
func MaskSinkURI(uri string) (string, error) {
	uriParsed, err := url.Parse(uri)
	if err != nil {
		log.Error("failed to parse sink URI", zap.Error(err))
		return "", err
	}
	queries := uriParsed.Query()
	if queries.Has("sasl-password") {
		queries.Set("sasl-password", "xxxxx")
		uriParsed.RawQuery = queries.Encode()
	}
	return uriParsed.Redacted(), nil
}

var sensitiveQueryParameterNames = []string{
	"password",
	"passwd",
	"pwd",
	"access",
	"token",
	"secret",
	"key",
	"signature",
	"credential",
	"private",
	"client",
}

// MaskSensitiveDataInURI returns an uri that sensitive infos has been masked.
func MaskSensitiveDataInURI(uri string) string {
	uriParsed, err := url.Parse(uri)
	if err != nil {
		log.Error("failed to parse sink URI", zap.Error(err))
		return ""
	}
	queries := uriParsed.Query()
	for key := range queries {
		lower := strings.ToLower(key)
		for _, secretKey := range sensitiveQueryParameterNames {
			if strings.Contains(lower, secretKey) {
				queries.Set(key, "xxxxx")
			}
		}
	}
	uriParsed.RawQuery = queries.Encode()
	return uriParsed.Redacted()
}
