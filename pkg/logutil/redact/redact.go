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

package redact

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/pingcap/errors"
	"go.uber.org/zap"
)

var (
	_ fmt.Stringer = redactStringer{}
)

// RedactionMode represents the type of log redaction
type RedactionMode int32

const (
	// RedactionModeOff disables redaction, shows all content
	RedactionModeOff RedactionMode = iota
	// RedactionModeOn enables redaction, replaces sensitive content with "?"
	RedactionModeOn
	// RedactionModeMarker enables redaction with markers, surrounds content with ‹›
	RedactionModeMarker
)

// String returns the string representation of RedactionMode
func (mode RedactionMode) String() string {
	switch mode {
	case RedactionModeOff:
		return "OFF"
	case RedactionModeOn:
		return "ON"
	case RedactionModeMarker:
		return "MARKER"
	default:
		return "UNKNOWN"
	}
}

// ParseRedactionMode parses a string into RedactionMode
func ParseRedactionMode(s string) (RedactionMode, error) {
	switch strings.ToUpper(s) {
	case "OFF", "FALSE", "0":
		return RedactionModeOff, nil
	case "ON", "TRUE", "1":
		return RedactionModeOn, nil
	case "MARKER":
		return RedactionModeMarker, nil
	default:
		return RedactionModeOff, errors.Errorf("invalid redaction mode: %s", s)
	}
}

var globalRedactionMode atomic.Value

func init() {
	globalRedactionMode.Store(RedactionModeOff)
}

// SetRedactionMode sets the global redaction mode
func SetRedactionMode(mode RedactionMode) {
	globalRedactionMode.Store(mode)
}

// GetRedactionMode returns the current global redaction mode
func GetRedactionMode() RedactionMode {
	return globalRedactionMode.Load().(RedactionMode)
}

// IsRedactionEnabled returns true if redaction is enabled (not OFF)
func IsRedactionEnabled() bool {
	return GetRedactionMode() != RedactionModeOff
}

// String redacts a string according to the current global redaction mode
func String(input string) string {
	mode := GetRedactionMode()
	switch mode {
	case RedactionModeMarker:
		b := &strings.Builder{}
		b.Grow(len(input) + 2)
		_, _ = b.WriteRune('‹')
		for _, c := range input {
			if c == '‹' || c == '›' {
				_, _ = b.WriteRune(c)
				_, _ = b.WriteRune(c)
			} else {
				_, _ = b.WriteRune(c)
			}
		}
		_, _ = b.WriteRune('›')
		return b.String()
	case RedactionModeOff:
		return input
	case RedactionModeOn:
		return "?"
	default:
		return "?"
	}
}

// StringWithMode redacts a string according to the specified redaction mode
func StringWithMode(mode RedactionMode, input string) string {
	switch mode {
	case RedactionModeMarker:
		b := &strings.Builder{}
		b.Grow(len(input) + 2)
		_, _ = b.WriteRune('‹')
		for _, c := range input {
			if c == '‹' || c == '›' {
				_, _ = b.WriteRune(c)
				_, _ = b.WriteRune(c)
			} else {
				_, _ = b.WriteRune(c)
			}
		}
		_, _ = b.WriteRune('›')
		return b.String()
	case RedactionModeOff:
		return input
	case RedactionModeOn:
		return "?"
	default:
		return "?"
	}
}

type redactStringer struct {
	mode     RedactionMode
	stringer fmt.Stringer
}

func (s redactStringer) String() string {
	return StringWithMode(s.mode, s.stringer.String())
}

// Stringer wraps a fmt.Stringer to apply redaction
func Stringer(input fmt.Stringer) redactStringer {
	return redactStringer{GetRedactionMode(), input}
}

// StringerWithMode wraps a fmt.Stringer to apply redaction with specified mode
func StringerWithMode(mode RedactionMode, input fmt.Stringer) redactStringer {
	return redactStringer{mode, input}
}

// Key redacts a key (byte slice) according to the current global redaction mode
func Key(key []byte) string {
	if IsRedactionEnabled() {
		return "?"
	}
	return strings.ToUpper(hex.EncodeToString(key))
}

// Value redacts a generic value according to the current global redaction mode
func Value(arg string) string {
	return String(arg)
}

// ZapString returns a zap field with redacted string content
func ZapString(key, value string) zap.Field {
	return zap.String(key, String(value))
}

// ZapStringer returns a zap field with redacted stringer content
func ZapStringer(key string, value fmt.Stringer) zap.Field {
	return zap.Stringer(key, Stringer(value))
}

// ZapKey returns a zap field with redacted key content
func ZapKey(key string, value []byte) zap.Field {
	return zap.String(key, Key(value))
}

// WriteRedact writes a redacted string to a strings.Builder
func WriteRedact(build *strings.Builder, v string, mode RedactionMode) {
	build.WriteString(StringWithMode(mode, v))
}

// DeRedactFile removes redaction markers from a file
func DeRedactFile(remove bool, input string, output string) error {
	ifile, err := os.Open(filepath.Clean(input))
	if err != nil {
		return errors.WithStack(err)
	}
	defer ifile.Close()

	var ofile io.Writer
	if output == "-" {
		ofile = os.Stdout
	} else {
		//nolint: gosec
		file, err := os.OpenFile(filepath.Clean(output), os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return errors.WithStack(err)
		}
		defer file.Close()
		ofile = file
	}

	return DeRedact(remove, ifile, ofile, "\n")
}

// DeRedact removes redaction markers from an io.Reader and writes to io.Writer
func DeRedact(remove bool, input io.Reader, output io.Writer, sep string) error {
	sc := bufio.NewScanner(input)
	out := bufio.NewWriter(output)
	defer out.Flush()
	buf := bytes.NewBuffer(nil)
	s := bufio.NewReader(nil)

	for sc.Scan() {
		s.Reset(strings.NewReader(sc.Text()))
		start := false
		for {
			ch, _, err := s.ReadRune()
			if err == io.EOF {
				break
			}
			if err != nil {
				return errors.WithStack(err)
			}
			if ch == '‹' {
				if start {
					// must be '<'
					pch, _, err := s.ReadRune()
					if err != nil {
						return errors.WithStack(err)
					}
					if pch == ch {
						_, _ = buf.WriteRune(ch)
					} else {
						_, _ = buf.WriteRune(ch)
						_, _ = buf.WriteRune(pch)
					}
				} else {
					start = true
					buf.Reset()
				}
			} else if ch == '›' {
				if start {
					// peek the next
					pch, _, err := s.ReadRune()
					if err != nil && err != io.EOF {
						return errors.WithStack(err)
					}
					if pch == ch {
						_, _ = buf.WriteRune(ch)
					} else {
						start = false
						if err != io.EOF {
							// unpeek it
							if err := s.UnreadRune(); err != nil {
								return errors.WithStack(err)
							}
						}
						if remove {
							_ = out.WriteByte('?')
						} else {
							_, err = io.Copy(out, buf)
							if err != nil {
								return errors.WithStack(err)
							}
						}
					}
				} else {
					_, _ = out.WriteRune(ch)
				}
			} else if start {
				_, _ = buf.WriteRune(ch)
			} else {
				_, _ = out.WriteRune(ch)
			}
		}
		if start {
			_, _ = out.WriteRune('‹')
			_, _ = out.WriteString(buf.String())
		}
		_, _ = out.WriteString(sep)
	}

	return nil
}
