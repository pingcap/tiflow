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

package model

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
)

// QuoteSchema quotes a full table name
func QuoteSchema(schema string, table string) string {
	return fmt.Sprintf("`%s`.`%s`", EscapeName(schema), EscapeName(table))
}

// QuoteName wraps a name with "`"
func QuoteName(name string) string {
	return "`" + EscapeName(name) + "`"
}

// EscapeName replaces all "`" in name with "``"
func EscapeName(name string) string {
	return strings.Replace(name, "`", "``", -1)
}

// HolderString returns a string of place holders separated by comma
func HolderString(n int) string {
	var builder strings.Builder
	builder.Grow((n-1)*2 + 1)
	for i := 0; i < n; i++ {
		if i > 0 {
			builder.WriteString(",")
		}
		builder.WriteString("?")
	}
	return builder.String()
}

// ExtractKeySuffix extracts the suffix of an etcd key, such as extracting
// "6a6c6dd290bc8732" from /tidb/cdc/changefeed/config/6a6c6dd290bc8732
func ExtractKeySuffix(key string) (string, error) {
	subs := strings.Split(key, "/")
	if len(subs) < 2 {
		return "", errors.Errorf("invalid key: %s", key)
	}
	return subs[len(subs)-1], nil
}
