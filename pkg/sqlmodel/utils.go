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

package sqlmodel

import (
	"fmt"
	"strings"
)

// valuesHolder gens values holder like (?,?,?).
func valuesHolder(n int) string {
	var builder strings.Builder
	builder.Grow((n-1)*2 + 3)
	builder.WriteByte('(')
	for i := 0; i < n; i++ {
		if i > 0 {
			builder.WriteString(",")
		}
		builder.WriteString("?")
	}
	builder.WriteByte(')')
	return builder.String()
}

// ColValAsStr convert column value as string
func ColValAsStr(v interface{}) string {
	switch dv := v.(type) {
	case []byte:
		return string(dv)
	case string:
		return dv
	}
	return fmt.Sprintf("%v", v)
}
