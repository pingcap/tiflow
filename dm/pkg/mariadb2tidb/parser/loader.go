// Copyright 2025 PingCAP, Inc.
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

package parser

import (
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver" // required: register TiDB SQL driver for parser
	"github.com/pingcap/tiflow/dm/pkg/mariadb2tidb/config"
	"github.com/pingcap/tiflow/dm/pkg/mariadb2tidb/utils"
	"go.uber.org/zap"
)

// Loader handles loading and parsing SQL files
type Loader struct {
	parser       *parser.Parser
	logger       *zap.Logger
	charsetMap   map[string]string // maps source charset to target charset
	collationMap map[string]string // maps source collation to target collation
}

// NewLoader creates a new SQL loader
func NewLoader() *Loader {
	return &Loader{
		parser:       parser.New(),
		logger:       utils.GetLogger(),
		charsetMap:   make(map[string]string),
		collationMap: make(map[string]string),
	}
}

// WithCharsetMappings configures the loader with charset and collation mappings
func (l *Loader) WithCharsetMappings(charsetMap map[string]string, collationMap map[string]string) *Loader {
	l.charsetMap = charsetMap
	l.collationMap = collationMap
	return l
}

// NewLoaderWithConfig creates a loader configured with charset mappings from config
func NewLoaderWithConfig(cfg *config.Config) *Loader {
	loader := NewLoader()

	if cfg != nil {
		// Convert charset mappings
		charsetMap := make(map[string]string)
		for source, mapping := range cfg.CharsetMappings {
			charsetMap[source] = mapping.TargetCharset
		}

		// Use collation mappings directly
		loader.WithCharsetMappings(charsetMap, cfg.CollationMappings)
	}

	return loader
}

// LoadFromFile loads and parses SQL from a file
func (l *Loader) LoadFromFile(filename string) ([]ast.StmtNode, error) {
	l.logger.Info("Loading SQL file", zap.String("file", filename))

	file, err := os.Open(filename)
	if err != nil {
		l.logger.Error("Failed to open file", zap.String("file", filename), zap.Error(err))
		return nil, err
	}
	defer file.Close()

	return l.LoadFromReader(file)
}

// LoadFromReader loads and parses SQL from an io.Reader
func (l *Loader) LoadFromReader(reader io.Reader) ([]ast.StmtNode, error) {
	content, err := io.ReadAll(reader)
	if err != nil {
		l.logger.Error("Failed to read SQL content", zap.Error(err))
		return nil, err
	}

	return l.LoadFromString(string(content))
}

// LoadFromString loads and parses SQL from a string
func (l *Loader) LoadFromString(sql string) ([]ast.StmtNode, error) {
	l.logger.Debug("Parsing SQL", zap.Int("length", len(sql)))

	// Preprocess unsupported constructs before parsing
	sql = l.preprocessSQL(sql)

	stmts, _, err := l.parser.Parse(sql, "", "")
	if err != nil {
		l.logger.Error("Failed to parse SQL", zap.Error(err))
		return nil, err
	}

	l.logger.Info("Successfully parsed SQL", zap.Int("statements", len(stmts)))
	return stmts, nil
}

var (
	uuidRegex      = regexp.MustCompile(`(?i)\buuid\b`)
	encryptedRegex = regexp.MustCompile("(?i)\\s*`encrypted`\\s*=\\s*yes\\s*`encryption_key_id`\\s*=\\s*\\d+\\s*")
	char36Regex    = regexp.MustCompile(`(?i)char\(36\)`)
	uuidKeyRegex   = regexp.MustCompile("(?i)unique\\s+key\\s+`?uuid`?")
)

// preprocessSQL performs lightweight text-based transformations before AST parsing.
// Currently handles trailing commas, UUID normalization, and charset/collation updates.
func (l *Loader) preprocessSQL(sql string) string {
	sql = stripTrailingCommas(sql)

	// Remove MariaDB table encryption options that TiDB doesn't support
	sql = encryptedRegex.ReplaceAllString(sql, " ")

	// Apply charset and collation transformations if configured
	sql = l.applyCharsetMappings(sql)

	// Replace standalone UUID data types with char(36) but keep functions and identifiers
	matches := uuidRegex.FindAllStringIndex(sql, -1)
	if matches == nil {
		return sql
	}

	var result strings.Builder
	last := 0
	for _, m := range matches {
		start, end := m[0], m[1]
		result.WriteString(sql[last:start])

		// Find preceding non-space/non-backtick character
		j := start - 1
		for j >= 0 && (isSpace(sql[j]) || sql[j] == '`') {
			j--
		}

		// Find following non-space/non-backtick character
		i := end
		for i < len(sql) && (isSpace(sql[i]) || sql[i] == '`') {
			i++
		}

		// Extract preceding word for context checks
		wordEnd := j
		for wordEnd >= 0 && (isAlphaNum(sql[wordEnd]) || sql[wordEnd] == '_') {
			wordEnd--
		}
		precedingWord := strings.ToLower(sql[wordEnd+1 : j+1])

		switch {
		case i < len(sql) && sql[i] == '(':
			// uuid used as function - leave unchanged
			result.WriteString(sql[start:end])
		case i < len(sql) && sql[i] == '`':
			// uuid inside backticks - identifier
			result.WriteString(sql[start:end])
		case precedingWord == "key" || precedingWord == "unique" || precedingWord == "primary" || precedingWord == "constraint" || precedingWord == "index":
			// index or constraint name - leave unchanged
			result.WriteString(sql[start:end])
		case j >= 0 && (isAlphaNum(sql[j]) || sql[j] == '_'):
			// uuid used as a data type - replace
			result.WriteString("char(36)")
		default:
			// uuid as column name or other - leave unchanged
			result.WriteString(sql[start:end])
		}

		last = end
	}

	result.WriteString(sql[last:])

	processed := result.String()

	// Rename unique key named "uuid" to "uuid_key"
	processed = uuidKeyRegex.ReplaceAllString(processed, "UNIQUE KEY uuid_key")

	// Add default '' to char(36) NOT NULL columns missing an explicit default
	matches = char36Regex.FindAllStringIndex(processed, -1)
	if matches == nil {
		return processed
	}

	var out strings.Builder
	last = 0
	for _, m := range matches {
		end := m[1]
		out.WriteString(processed[last:end])

		rest := processed[end:]
		segEnd := strings.IndexAny(rest, ",)")
		if segEnd == -1 {
			segEnd = len(rest)
		}
		segment := rest[:segEnd]
		lowerSeg := strings.ToLower(segment)
		if strings.Contains(lowerSeg, "not null") && !strings.Contains(lowerSeg, "default") {
			idx := strings.Index(lowerSeg, "not null") + len("not null")
			segment = segment[:idx] + " default ''" + segment[idx:]
		}
		out.WriteString(segment)
		last = end + segEnd
	}
	out.WriteString(processed[last:])
	return out.String()
}

// applyCharsetMappings applies configured charset and collation transformations
func (l *Loader) applyCharsetMappings(sql string) string {
	// Apply charset mappings
	for sourceCharset, targetCharset := range l.charsetMap {
		charsetRegex := regexp.MustCompile(`(?i)character\s+set\s+` + regexp.QuoteMeta(sourceCharset) + `\b`)
		sql = charsetRegex.ReplaceAllString(sql, "CHARACTER SET "+targetCharset)
	}

	// Apply collation mappings
	for sourceCollation, targetCollation := range l.collationMap {
		collationRegex := regexp.MustCompile(`(?i)collate\s+` + regexp.QuoteMeta(sourceCollation) + `\b`)
		sql = collationRegex.ReplaceAllString(sql, "COLLATE "+targetCollation)
	}

	return sql
}

func stripTrailingCommas(sql string) string {
	var out strings.Builder
	out.Grow(len(sql))

	inSingle := false
	inDouble := false
	inBacktick := false
	inLineComment := false
	inBlockComment := false

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		if inLineComment {
			out.WriteByte(ch)
			if ch == '\n' {
				inLineComment = false
			}
			continue
		}

		if inBlockComment {
			out.WriteByte(ch)
			if ch == '*' && i+1 < len(sql) && sql[i+1] == '/' {
				out.WriteByte(sql[i+1])
				i++
				inBlockComment = false
			}
			continue
		}

		if inSingle {
			out.WriteByte(ch)
			if ch == '\\' && i+1 < len(sql) {
				out.WriteByte(sql[i+1])
				i++
				continue
			}
			if ch == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					out.WriteByte(sql[i+1])
					i++
					continue
				}
				inSingle = false
			}
			continue
		}

		if inDouble {
			out.WriteByte(ch)
			if ch == '\\' && i+1 < len(sql) {
				out.WriteByte(sql[i+1])
				i++
				continue
			}
			if ch == '"' {
				if i+1 < len(sql) && sql[i+1] == '"' {
					out.WriteByte(sql[i+1])
					i++
					continue
				}
				inDouble = false
			}
			continue
		}

		if inBacktick {
			out.WriteByte(ch)
			if ch == '`' {
				if i+1 < len(sql) && sql[i+1] == '`' {
					out.WriteByte(sql[i+1])
					i++
					continue
				}
				inBacktick = false
			}
			continue
		}

		if isLineCommentStart(sql, i) {
			out.WriteByte(ch)
			out.WriteByte(sql[i+1])
			i++
			inLineComment = true
			continue
		}

		if ch == '#' {
			out.WriteByte(ch)
			inLineComment = true
			continue
		}

		if isBlockCommentStart(sql, i) {
			out.WriteByte(ch)
			out.WriteByte(sql[i+1])
			i++
			inBlockComment = true
			continue
		}

		switch ch {
		case '\'':
			inSingle = true
			out.WriteByte(ch)
			continue
		case '"':
			inDouble = true
			out.WriteByte(ch)
			continue
		case '`':
			inBacktick = true
			out.WriteByte(ch)
			continue
		}

		if ch == ',' && isTrailingComma(sql, i+1) {
			continue
		}

		out.WriteByte(ch)
	}

	return out.String()
}

func isTrailingComma(sql string, start int) bool {
	i := start
	for i < len(sql) {
		if isSpace(sql[i]) {
			i++
			continue
		}
		if isLineCommentStart(sql, i) {
			i = skipLineComment(sql, i)
			continue
		}
		if sql[i] == '#' {
			i = skipLineComment(sql, i)
			continue
		}
		if isBlockCommentStart(sql, i) {
			i = skipBlockComment(sql, i)
			continue
		}
		break
	}
	return i < len(sql) && sql[i] == ')'
}

func isLineCommentStart(sql string, i int) bool {
	if i+1 >= len(sql) || sql[i] != '-' || sql[i+1] != '-' {
		return false
	}
	if i+2 >= len(sql) {
		return true
	}
	return isSpace(sql[i+2])
}

func isBlockCommentStart(sql string, i int) bool {
	return i+1 < len(sql) && sql[i] == '/' && sql[i+1] == '*'
}

func skipLineComment(sql string, i int) int {
	for i < len(sql) && sql[i] != '\n' {
		i++
	}
	return i
}

func skipBlockComment(sql string, i int) int {
	i += 2
	for i < len(sql) {
		if sql[i] == '*' && i+1 < len(sql) && sql[i+1] == '/' {
			return i + 2
		}
		i++
	}
	return len(sql)
}

// isSpace reports whether b is an ASCII whitespace character.
func isSpace(b byte) bool {
	switch b {
	case ' ', '\t', '\n', '\r', '\v', '\f':
		return true
	default:
		return false
	}
}

// isAlphaNum reports whether b is an ASCII letter or digit.
func isAlphaNum(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9')
}
