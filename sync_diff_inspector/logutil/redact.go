// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logutil

import (
	"fmt"

	dmlog "github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

// ZapRedactError omits error details when redact log is enabled,
// otherwise it is the same as zap.Error.
func ZapRedactError(err error) zap.Field {
	if err == nil || !dmlog.IsRedactLogEnabled() {
		return zap.Error(err)
	}
	return zap.String("error", "?")
}

// ZapRedactReflect omits the reflected value when redact log is enabled,
// otherwise it is the same as zap.Reflect.
func ZapRedactReflect(key string, arg any) zap.Field {
	if dmlog.IsRedactLogEnabled() {
		return zap.String(key, "?")
	}
	return zap.Reflect(key, arg)
}

// ZapRedactStringer omits the string value when redact log is enabled,
// otherwise it is the same as zap.Stringer.
func ZapRedactStringer(key string, arg fmt.Stringer) zap.Field {
	if dmlog.IsRedactLogEnabled() {
		return zap.String(key, "?")
	}
	return zap.Stringer(key, arg)
}

// ZapRedactStrings omits the string slice when redact log is enabled,
// otherwise it is the same as zap.Strings.
func ZapRedactStrings(key string, args []string) zap.Field {
	if dmlog.IsRedactLogEnabled() {
		return zap.String(key, "?")
	}
	return zap.Strings(key, args)
}
