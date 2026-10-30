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

package redact

import (
	"io"
	"regexp"
	"strings"

	"github.com/pingcap/tidb/pkg/objstore"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

const invalidConfig = "(invalid config; redacted)"

// YAMLConfig hides storage URL credentials in a single configuration
// document, retaining unknown fields and leaving the original request untouched.
func YAMLConfig(input string) string {
	if input == "" {
		return ""
	}
	var cfg map[string]any
	d := yaml.NewDecoder(strings.NewReader(input))
	d.SetStrict(true)
	if err := d.Decode(&cfg); err != nil {
		return invalidConfig
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return invalidConfig
	}
	redactConfigURL("", cfg)
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return invalidConfig
	}
	return string(data)
}

// redactConfigURL only mutates decoded diagnostic copies. URL credential rules
// belong to objstore.RedactURL; other configuration fields are left unchanged.
func redactConfigURL(key string, value any) any {
	switch strings.ToLower(key) {
	case "dir", "data-dir", "data_dir", "output-dir", "outputdir":
		if raw, ok := value.(string); ok {
			return objstore.RedactURL(raw)
		}
		// Requests are logged before schema validation. Do not echo malformed
		// URL fields, which can contain credentials inside arrays or objects.
		if value != nil {
			return objstore.InvalidURLPlaceholder
		}
	}
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			v[key] = redactConfigURL(key, child)
		}
	case map[any]any:
		for key, child := range v {
			name, _ := key.(string)
			v[key] = redactConfigURL(name, child)
		}
	case []any:
		for _, child := range v {
			redactConfigURL("", child)
		}
	}
	return value
}

// diagnosticURL only locates URLs in free-form text. Credential selection and
// malformed-URL handling belong to TiDB's objstore.RedactURL, not this package.
// A single slash also matches paths normalized by filepath, such as s3:/bucket.
// It stops at apostrophes, so adjacent quoted URLs are redacted separately.
var diagnosticURL = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.-]*:/{1,2}[^\s"'<>]+`)

// apostropheURL also spans apostrophes, which are valid in URL paths and
// userinfo, so credentials after them are not cut off from their URL.
var apostropheURL = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.-]*:/{1,2}[^\s"<>]+`)

// escapedAmpersand matches '&' as escaped by encoding/json (\u0026), with
// more backslashes when that JSON is itself embedded in a JSON string.
var escapedAmpersand = regexp.MustCompile(`\\+u0026`)

// Text hides the credentials of the storage URLs embedded in diagnostic text,
// using the same TiDB URL policy as objstore.RedactURL.
func Text(text string) string {
	if !strings.Contains(text, "'") {
		return diagnosticURL.ReplaceAllStringFunc(text, redactTextURL)
	}
	text = redactBeforeApostrophes(text)
	return apostropheURL.ReplaceAllStringFunc(text, func(match string) string {
		redacted := redactTextURL(match)
		// The first pass also defers invalid URLs followed only by a closing quote.
		if !strings.Contains(strings.TrimRight(match, `\'`), "'") &&
			!strings.HasPrefix(redacted, objstore.InvalidURLPlaceholder) {
			return match
		}
		return redacted
	})
}

// redactBeforeApostrophes redacts the URLs that end at a delimiter. A URL cut
// off by an apostrophe that cannot be parsed, such as one with an apostrophe
// in its password, is kept, so that it is redacted as a whole together with
// its remainder instead of leaving the remainder unredacted.
func redactBeforeApostrophes(text string) string {
	var b strings.Builder
	last := 0
	for _, loc := range diagnosticURL.FindAllStringIndex(text, -1) {
		match := text[loc[0]:loc[1]]
		redacted := redactTextURL(match)
		if strings.HasPrefix(text[loc[1]:], "'") &&
			strings.HasPrefix(redacted, objstore.InvalidURLPlaceholder) {
			continue
		}
		b.WriteString(text[last:loc[0]])
		b.WriteString(redacted)
		last = loc[1]
	}
	b.WriteString(text[last:])
	return b.String()
}

// redactTextURL restores JSON-escaped query separators before redaction.
// Otherwise every parameter after the first one is parsed as part of the
// first value, and is kept whenever the first parameter is not sensitive.
func redactTextURL(match string) string {
	// Trailing backslashes escape the delimiter that ended the match, such as
	// a quote in nested JSON, and a trailing apostrophe closes a quoted URL,
	// so they are not part of the URL.
	raw := strings.TrimRight(match, `\'`)
	suffix := match[len(raw):]
	escaped := escapedAmpersand.FindString(raw)
	if escaped == "" {
		return objstore.RedactURL(raw) + suffix
	}
	redacted := objstore.RedactURL(escapedAmpersand.ReplaceAllLiteralString(raw, "&"))
	return strings.ReplaceAll(redacted, "&", escaped) + suffix
}

// Error replaces zap.Error for errors that may contain storage URLs, such as
// storage SDK errors, and hides the credentials of these URLs. It formats the
// error without exposing an unredacted verbose representation, and never
// changes the error used for execution or retries.
func Error(err error) zap.Field {
	if err == nil {
		return zap.Skip()
	}
	return zap.String("error", Text(err.Error()))
}

// ProxyURL hides the credentials of a proxy URL. It adapts scheme-less proxy
// syntax before using TiDB's URL redactor.
// Without the temporary scheme, user:password@host:port is treated as a local
// path by objstore.RedactURL and is returned unchanged.
func ProxyURL(raw string) string {
	if !strings.Contains(raw, "://") {
		return strings.TrimPrefix(objstore.RedactURL("http://"+raw), "http://")
	}
	return objstore.RedactURL(raw)
}
