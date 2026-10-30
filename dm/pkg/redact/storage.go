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

// YAMLConfig hides storage URL credentials in a single YAML configuration.
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

func redactConfigURL(key string, value any) any {
	switch strings.ToLower(key) {
	case "dir", "data-dir", "data_dir", "output-dir", "outputdir":
		if raw, ok := value.(string); ok {
			return objstore.RedactURL(raw)
		}
		// A malformed URL field may hide credentials inside arrays or objects.
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

// diagnosticURL matches URLs in text, including s3:/bucket normalized by
// filepath. It stops at apostrophes to split adjacent quoted URLs.
var diagnosticURL = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.-]*:/{1,2}[^\s"'<>]+`)

// apostropheURL also spans apostrophes, which are valid in URL paths and userinfo.
var apostropheURL = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.-]*:/{1,2}[^\s"<>]+`)

// escapedAmpersand matches '&' escaped by encoding/json, possibly nested.
var escapedAmpersand = regexp.MustCompile(`\\+u0026`)

// Text hides the credentials of the storage URLs in text.
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

// redactBeforeApostrophes redacts the URLs that end at a delimiter. An invalid
// URL cut off by an apostrophe is kept for Text to redact as a whole.
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

// redactTextURL redacts a matched URL, keeping its JSON-escaped query
// separators and the trailing backslashes and apostrophe that end it.
func redactTextURL(match string) string {
	raw := strings.TrimRight(match, `\'`)
	suffix := match[len(raw):]
	escaped := escapedAmpersand.FindString(raw)
	if escaped == "" {
		return objstore.RedactURL(raw) + suffix
	}
	redacted := objstore.RedactURL(escapedAmpersand.ReplaceAllLiteralString(raw, "&"))
	return strings.ReplaceAll(redacted, "&", escaped) + suffix
}

// Error is zap.Error with the credentials of storage URLs in err hidden.
func Error(err error) zap.Field {
	if err == nil {
		return zap.Skip()
	}
	return zap.String("error", Text(err.Error()))
}

// ProxyURL hides the credentials of a proxy URL, which may omit the scheme.
func ProxyURL(raw string) string {
	if !strings.Contains(raw, "://") {
		return strings.TrimPrefix(objstore.RedactURL("http://"+raw), "http://")
	}
	return objstore.RedactURL(raw)
}
