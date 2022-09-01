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

package logutil

import (
	"regexp"
)

var (
	passwordPatterns = `(password: (\\")?)(.*?)((\\")?\\n)`
	passwordRegexp   = regexp.MustCompile(passwordPatterns)

	sslPatterns = `(ssl-(ca|key|cert)-bytes:)((\\n\s{4}-\s\d+)+)`
	sslRegexp   = regexp.MustCompile(sslPatterns)

	// to match PEM format, ref: https://en.wikipedia.org/wiki/Privacy-Enhanced_Mail
	beginPattern      = `(.*?)(-{5})BEGIN( [A-Z]+)+(-{5})`
	endPattern        = `(.*?)(-{5})END( [A-Z]+)+(-{5})[\"\']?`
	sslStringPatterns = `(ssl-(ca|key|cert)-bytes:)` + beginPattern + endPattern
	sslStringRegexp   = regexp.MustCompile(sslStringPatterns)

	// HideSensitive is used to replace sensitive information with `******` in log.
	HideSensitive = func(input string) string {
		output := passwordRegexp.ReplaceAllString(input, "$1******$4")
		output = sslRegexp.ReplaceAllString(output, "$1 \"******\"")
		output = sslStringRegexp.ReplaceAllString(output, "$1 \"******\"")
		return output
	}
)
