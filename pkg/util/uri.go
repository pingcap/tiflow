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
	"context"
	"errors"
	"net"
	"net/url"
	"strings"

	"github.com/pingcap/tiflow/pkg/sink"
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
	uriParsed, err := parseURIForMasking(uri)
	if err != nil {
		return "", MaskSensitiveDataInURLError(err)
	}
	queries, queryErr := url.ParseQuery(uriParsed.RawQuery)
	masked := queryErr != nil
	for key := range queries {
		if isSensitiveQueryParameter(key) {
			queries.Set(key, "xxxxx")
			masked = true
		}
	}
	if masked {
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
	uriParsed, err := parseURIForMasking(uri)
	if err != nil {
		return ""
	}
	queries := uriParsed.Query()
	for key := range queries {
		if isSensitiveQueryParameter(key) {
			queries.Set(key, "xxxxx")
		}
	}
	uriParsed.RawQuery = queries.Encode()
	return uriParsed.Redacted()
}

func parseURIForMasking(uri string) (*url.URL, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	if parsed.Opaque != "" {
		return nil, errors.New("invalid opaque URI")
	}

	scheme := parsed.Scheme
	requiresAuthority := scheme == "http" || scheme == "https" || sink.IsMQScheme(scheme) ||
		(sink.IsStorageScheme(scheme) && scheme != sink.FileScheme && scheme != sink.CloudStorageNoopScheme)
	if sink.IsMySQLCompatibleScheme(scheme) {
		// The MySQL sink supports a default address in URLs like mysql://.
		requiresAuthority = parsed.Path != "" || parsed.Fragment != ""
	}
	if parsed.Host == "" && requiresAuthority {
		return nil, errors.New("network URI is missing authority")
	}
	return parsed, nil
}

func isSensitiveQueryParameter(key string) bool {
	lower := strings.ToLower(key)
	if lower == "kafka-client-id" {
		return false
	}
	for _, secretKey := range sensitiveQueryParameterNames {
		if strings.Contains(lower, secretKey) {
			return true
		}
	}
	return false
}

// HasMaskedSensitiveDataInURI reports whether a URI still carries a masked
// credential from a read response.
func HasMaskedSensitiveDataInURI(uri string) bool {
	parsed, err := url.Parse(uri)
	if err != nil {
		return false
	}
	if parsed.User != nil {
		if password, ok := parsed.User.Password(); ok && password == "xxxxx" {
			return true
		}
	}
	for key, values := range parsed.Query() {
		if isSensitiveQueryParameter(key) {
			for _, value := range values {
				if value == "xxxxx" {
					return true
				}
			}
		}
	}
	return false
}

// MaskSensitiveDataInURIForError masks sensitive data in a URI for error messages.
func MaskSensitiveDataInURIForError(uri string) string {
	maskedURI := MaskSensitiveDataInURI(uri)
	if maskedURI == "" && uri != "" {
		return "<invalid uri>"
	}
	return maskedURI
}

// MaskSensitiveDataInURLError masks a net/url error's URL and sanitizes its cause.
func MaskSensitiveDataInURLError(err error) error {
	if err == nil {
		return nil
	}
	var urlErr *url.Error
	if !errors.As(err, &urlErr) {
		return err
	}
	return &url.Error{
		Op:  urlErr.Op,
		URL: MaskSensitiveDataInURIForError(urlErr.URL),
		Err: sanitizeURLErrorCause(urlErr.Op, urlErr.Err),
	}
}

func sanitizeURLErrorCause(op string, err error) error {
	// Parser and redirect errors can quote the input, including credentials.
	// Use fixed descriptions so the cause cannot reintroduce that text.
	var message string
	if err != nil {
		message = err.Error()
	}
	var escapeErr url.EscapeError
	var hostErr url.InvalidHostError
	switch {
	case errors.Is(err, context.Canceled):
		return context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded
	case strings.HasPrefix(message, "failed to parse Location header "):
		return errors.New("invalid redirect Location header")
	case strings.HasPrefix(message, "invalid port "):
		return errors.New("invalid port")
	case errors.As(err, &escapeErr):
		return errors.New("invalid URL escape")
	case errors.As(err, &hostErr):
		return errors.New("invalid URL host")
	case op == "parse":
		return errors.New("invalid URL")
	default:
		return errors.New("URL request failed")
	}
}
