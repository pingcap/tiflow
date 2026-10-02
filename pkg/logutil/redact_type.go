// Copyright 2024 PingCAP, Inc.
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
	"encoding/json"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/logutil/redact"
)

// RedactInfoLogType is the behavior of redacting sensitive information in logs.
// It follows the same pattern as PD's implementation.
type RedactInfoLogType int

func (t RedactInfoLogType) Type() string {
	return "redactLog"
}

const (
	// RedactInfoLogOFF means log redaction is disabled.
	RedactInfoLogOFF RedactInfoLogType = iota
	// RedactInfoLogON means log redaction is enabled, and will replace the sensitive information with "?".
	RedactInfoLogON
	// RedactInfoLogMarker means log redaction is enabled, and will use single guillemets ‹› to enclose the sensitive information.
	RedactInfoLogMarker
)

const invalidRedactInfoLogTypeErrMsg = `the "redact-info-log" value is invalid; it should be either false, true, or "MARKER"`

// String returns the string representation of RedactInfoLogType
func (t RedactInfoLogType) String() string {
	switch t {
	case RedactInfoLogOFF:
		return "OFF"
	case RedactInfoLogON:
		return "ON"
	case RedactInfoLogMarker:
		return "MARKER"
	default:
		return "UNKNOWN"
	}
}

// UnmarshalJSON implements the json.Unmarshaler interface
func (t *RedactInfoLogType) UnmarshalJSON(data []byte) error {
	// First try to unmarshal as string to check for "MARKER"
	var s string
	err := json.Unmarshal(data, &s)
	if err == nil && strings.ToUpper(s) == "MARKER" {
		*t = RedactInfoLogMarker
		return nil
	}

	// Then try to unmarshal as boolean
	var b bool
	err = json.Unmarshal(data, &b)
	if err != nil {
		return errors.New(invalidRedactInfoLogTypeErrMsg)
	}

	if b {
		*t = RedactInfoLogON
	} else {
		*t = RedactInfoLogOFF
	}
	return nil
}

// MarshalJSON implements the json.Marshaler interface
func (t RedactInfoLogType) MarshalJSON() ([]byte, error) {
	switch t {
	case RedactInfoLogOFF:
		return json.Marshal(false)
	case RedactInfoLogON:
		return json.Marshal(true)
	case RedactInfoLogMarker:
		return json.Marshal("MARKER")
	default:
		return nil, errors.Errorf("invalid RedactInfoLogType: %d", int(t))
	}
}

// UnmarshalTOML implements the TOML unmarshaler interface
func (t *RedactInfoLogType) UnmarshalTOML(data interface{}) error {
	switch v := data.(type) {
	case bool:
		if v {
			*t = RedactInfoLogON
		} else {
			*t = RedactInfoLogOFF
		}
		return nil
	case string:
		if strings.ToUpper(v) == "MARKER" {
			*t = RedactInfoLogMarker
			return nil
		}
		return errors.New(invalidRedactInfoLogTypeErrMsg)
	default:
		return errors.New(invalidRedactInfoLogTypeErrMsg)
	}
}

// MarshalTOML implements the TOML marshaler interface
func (t RedactInfoLogType) MarshalTOML() (interface{}, error) {
	switch t {
	case RedactInfoLogOFF:
		return false, nil
	case RedactInfoLogON:
		return true, nil
	case RedactInfoLogMarker:
		return "MARKER", nil
	default:
		return nil, errors.Errorf("invalid RedactInfoLogType: %d", int(t))
	}
}

// UnmarshalText implements the encoding.TextUnmarshaler interface
func (t *RedactInfoLogType) UnmarshalText(text []byte) error {
	s := strings.ToUpper(string(text))
	switch s {
	case "FALSE", "OFF", "0":
		*t = RedactInfoLogOFF
	case "TRUE", "ON", "1":
		*t = RedactInfoLogON
	case "MARKER":
		*t = RedactInfoLogMarker
	default:
		return errors.New(invalidRedactInfoLogTypeErrMsg)
	}
	return nil
}

// MarshalText implements the encoding.TextMarshaler interface
func (t RedactInfoLogType) MarshalText() ([]byte, error) {
	switch t {
	case RedactInfoLogOFF:
		return []byte("false"), nil
	case RedactInfoLogON:
		return []byte("true"), nil
	case RedactInfoLogMarker:
		return []byte("MARKER"), nil
	default:
		return nil, errors.Errorf("invalid RedactInfoLogType: %d", int(t))
	}
}

// ToRedactionMode converts RedactInfoLogType to redact.RedactionMode
func (t RedactInfoLogType) ToRedactionMode() redact.RedactionMode {
	switch t {
	case RedactInfoLogOFF:
		return redact.RedactionModeOff
	case RedactInfoLogON:
		return redact.RedactionModeOn
	case RedactInfoLogMarker:
		return redact.RedactionModeMarker
	default:
		return redact.RedactionModeOff
	}
}

// FromRedactionMode converts redact.RedactionMode to RedactInfoLogType
func FromRedactionMode(mode redact.RedactionMode) RedactInfoLogType {
	switch mode {
	case redact.RedactionModeOff:
		return RedactInfoLogOFF
	case redact.RedactionModeOn:
		return RedactInfoLogON
	case redact.RedactionModeMarker:
		return RedactInfoLogMarker
	default:
		return RedactInfoLogOFF
	}
}

// InitLogRedactionFromType initializes log redaction from RedactInfoLogType
func InitLogRedactionFromType(redactType RedactInfoLogType) error {
	redactionMode := redactType.ToRedactionMode()
	redact.SetRedactionMode(redactionMode)
	return nil
}

// ParseRedactInfoLog parses a string value into RedactInfoLogType for backward compatibility
func ParseRedactInfoLog(value string) (RedactInfoLogType, error) {
	var t RedactInfoLogType
	err := t.UnmarshalText([]byte(value))
	if err != nil {
		return RedactInfoLogOFF, err
	}
	return t, nil
}

// ValidateRedactInfoLog validates that a string is a valid redact-info-log value
func ValidateRedactInfoLog(value string) error {
	_, err := ParseRedactInfoLog(value)
	return err
}

// Set implements the pflag.Value interface
func (t *RedactInfoLogType) Set(s string) error {
	return t.UnmarshalText([]byte(s))
}

// RedactInfoLogFromBoolAndMode creates RedactInfoLogType from legacy boolean and mode string
// This is for backward compatibility with the old separate config approach
func RedactInfoLogFromBoolAndMode(enabled bool, mode string) RedactInfoLogType {
	if !enabled {
		return RedactInfoLogOFF
	}

	switch strings.ToUpper(mode) {
	case "MARKER":
		return RedactInfoLogMarker
	case "ON", "TRUE", "1":
		return RedactInfoLogON
	default:
		return RedactInfoLogON // Default to ON when enabled but mode unclear
	}
}
