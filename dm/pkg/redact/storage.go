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
	"bytes"
	"encoding/json"
	"io"
	"regexp"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/tidb/pkg/objstore"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

const invalidConfig = "(invalid config; redacted)"

// ConfigFieldWithRedactedURL formats a diagnostic copy without exposing storage URL
// credentials. It must not be used to serialize execution or persisted configs.
func ConfigFieldWithRedactedURL(key string, cfg any) zap.Field {
	data, err := json.Marshal(cfg)
	if err != nil {
		return zap.String(key, invalidConfig)
	}
	var value any
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if err := d.Decode(&value); err != nil {
		return zap.String(key, invalidConfig)
	}
	redactConfigURL("", value)
	data, err = json.Marshal(value)
	if err != nil {
		return zap.String(key, invalidConfig)
	}
	return zap.String(key, string(data))
}

// YAMLConfigWithRedactedURL hides storage URL credentials in a single configuration
// document, retaining unknown fields and leaving the original request untouched.
func YAMLConfigWithRedactedURL(input string) string {
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

// TOMLConfigWithRedactedURL hides storage URL credentials in diagnostic TOML.
func TOMLConfigWithRedactedURL(input string) string {
	if input == "" {
		return ""
	}
	var cfg map[string]any
	if _, err := toml.Decode(input, &cfg); err != nil {
		return invalidConfig
	}
	redactConfigURL("", cfg)
	var out bytes.Buffer
	if err := toml.NewEncoder(&out).Encode(cfg); err != nil {
		return invalidConfig
	}
	return out.String()
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
	case []map[string]any:
		for _, child := range v {
			redactConfigURL("", child)
		}
	}
	return value
}

// diagnosticURL only locates URLs in free-form text. Credential selection and
// malformed-URL handling belong to TiDB's objstore.RedactURL, not this package.
// A single slash also matches paths normalized by filepath, such as s3:/bucket.
var diagnosticURL = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.-]*:/{1,2}[^\s"'<>]+`)

// escapedAmpersand matches '&' as escaped by encoding/json (\u0026), with
// more backslashes when that JSON is itself embedded in a JSON string.
var escapedAmpersand = regexp.MustCompile(`\\+u0026`)

// TextWithRedactedURL applies the same TiDB URL policy to URLs embedded in diagnostic text.
func TextWithRedactedURL(text string) string {
	return diagnosticURL.ReplaceAllStringFunc(text, redactTextURL)
}

// redactTextURL restores JSON-escaped query separators before redaction.
// Otherwise every parameter after the first one is parsed as part of the
// first value, and is kept whenever the first parameter is not sensitive.
func redactTextURL(match string) string {
	// Trailing backslashes escape the delimiter that ended the match, such as
	// a quote in nested JSON, so they are not part of the URL.
	raw := strings.TrimRight(match, `\`)
	suffix := match[len(raw):]
	escaped := escapedAmpersand.FindString(raw)
	if escaped == "" {
		return objstore.RedactURL(raw) + suffix
	}
	redacted := objstore.RedactURL(escapedAmpersand.ReplaceAllLiteralString(raw, "&"))
	return strings.ReplaceAll(redacted, "&", escaped) + suffix
}

// ErrorFieldWithRedactedURL formats a diagnostic error without exposing an unredacted verbose
// representation. It never changes the error used for execution or retries.
func ErrorFieldWithRedactedURL(err error) zap.Field {
	if err == nil {
		return zap.Skip()
	}
	return zap.String("error", TextWithRedactedURL(err.Error()))
}

// RedactedProxyURL adapts scheme-less proxy syntax before using TiDB's URL redactor.
// Without the temporary scheme, user:password@host:port is treated as a local
// path by objstore.RedactURL and is returned unchanged.
func RedactedProxyURL(raw string) string {
	if !strings.Contains(raw, "://") {
		return strings.TrimPrefix(objstore.RedactURL("http://"+raw), "http://")
	}
	return objstore.RedactURL(raw)
}
