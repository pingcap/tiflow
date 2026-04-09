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

package rules

import (
	"regexp"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/format"
)

type rawStatement struct {
	text      string
	originPos int
}

type sqlSegment struct {
	text   string
	isCode bool
}

func newRawStatement(text string) *rawStatement {
	return &rawStatement{text: text}
}

// Restore implements ast.Node.
func (s *rawStatement) Restore(ctx *format.RestoreCtx) error {
	ctx.WritePlain(s.text)
	return nil
}

// Accept implements ast.Node.
func (s *rawStatement) Accept(v ast.Visitor) (ast.Node, bool) {
	newNode, skipChildren := v.Enter(s)
	if skipChildren {
		return v.Leave(newNode)
	}
	return v.Leave(newNode)
}

// Text implements ast.Node.
func (s *rawStatement) Text() string {
	return s.text
}

// OriginalText implements ast.Node.
func (s *rawStatement) OriginalText() string {
	return s.text
}

// SetText implements ast.Node.
func (s *rawStatement) SetText(_ charset.Encoding, text string) {
	s.text = text
}

// SetOriginTextPosition implements ast.Node.
func (s *rawStatement) SetOriginTextPosition(offset int) {
	s.originPos = offset
}

// OriginTextPosition implements ast.Node.
func (s *rawStatement) OriginTextPosition() int {
	return s.originPos
}

// ApplyPreparseRules applies rules that operate on raw SQL text before parsing.
func ApplyPreparseRules(sql string, ruleList []Rule) (string, error) {
	if len(ruleList) == 0 {
		return sql, nil
	}

	stmt := newRawStatement(sql)
	var trailing []Rule
	for _, rule := range ruleList {
		if rule.Name() == "TrailingComma" {
			trailing = append(trailing, rule)
			continue
		}
		if !rule.ShouldApply(stmt) {
			continue
		}
		updated, err := rule.Apply(stmt)
		if err != nil {
			return sql, err
		}
		if raw, ok := updated.(*rawStatement); ok {
			stmt = raw
			continue
		}
		if updated != nil {
			stmt = newRawStatement(updated.Text())
		}
	}
	for _, rule := range trailing {
		if !rule.ShouldApply(stmt) {
			continue
		}
		updated, err := rule.Apply(stmt)
		if err != nil {
			return sql, err
		}
		if raw, ok := updated.(*rawStatement); ok {
			stmt = raw
			continue
		}
		if updated != nil {
			stmt = newRawStatement(updated.Text())
		}
	}

	return stmt.Text(), nil
}

var (
	versionMacroRegex = regexp.MustCompile(`(?s)/\*!\d+\s*(.*?)\*/`)

	withSystemVersioningRegex    = regexp.MustCompile(`(?i)\bWITH\s+SYSTEM\s+VERSIONING\b`)
	withoutSystemVersioningRegex = regexp.MustCompile(`(?i)\bWITHOUT\s+SYSTEM\s+VERSIONING\b`)
	periodSystemTimeRegex        = regexp.MustCompile(`(?i)\bPERIOD\s+FOR\s+SYSTEM_TIME\s*\([^)]*\)`)
	rowStartRegex                = regexp.MustCompile(`(?i)\bGENERATED\s+ALWAYS\s+AS\s+ROW\s+START\b`)
	rowEndRegex                  = regexp.MustCompile(`(?i)\bGENERATED\s+ALWAYS\s+AS\s+ROW\s+END\b`)
	alterSystemVersioningRegex   = regexp.MustCompile(`(?i)\b(ADD|DROP)\s+SYSTEM\s+VERSIONING\b`)

	columnAttributeRegex = regexp.MustCompile(`(?i)\b(INVISIBLE|COMPRESSED|PERSISTENT)\b`)

	sequenceTypeRegex      = regexp.MustCompile(`(?i)(\bCREATE\s+SEQUENCE\s+[^;]*?)\s+AS\s+\w+\b`)
	alterSequenceTypeRegex = regexp.MustCompile(`(?i)(\bALTER\s+SEQUENCE\s+[^;]*?)\s+AS\s+\w+\b`)

	createOrReplaceTableRegex    = regexp.MustCompile(`(?i)\bCREATE\s+OR\s+REPLACE\s+TABLE\s+([^\s(]+)`)
	createOrReplaceSequenceRegex = regexp.MustCompile(`(?i)\bCREATE\s+OR\s+REPLACE\s+SEQUENCE\s+([^\s(]+)`)
	createOrReplaceIndexRegex    = regexp.MustCompile(`(?i)\bCREATE\s+OR\s+REPLACE\s+INDEX\s+([^\s]+)\s+ON\s+([^\s(]+)`)
)

func hasVersionMacros(sql string) bool {
	return MatchOutsideQuotedAndComments(sql, versionMacroRegex.MatchString)
}

func stripVersionMacros(sql string) string {
	return TransformOutsideQuotedAndComments(sql, func(segment string) string {
		return versionMacroRegex.ReplaceAllString(segment, "$1")
	})
}

func hasSystemVersioning(sql string) bool {
	return MatchOutsideQuotedAndComments(sql, func(segment string) bool {
		return withSystemVersioningRegex.MatchString(segment) ||
			withoutSystemVersioningRegex.MatchString(segment) ||
			periodSystemTimeRegex.MatchString(segment) ||
			rowStartRegex.MatchString(segment) ||
			rowEndRegex.MatchString(segment) ||
			alterSystemVersioningRegex.MatchString(segment)
	})
}

func stripSystemVersioning(sql string) string {
	return TransformOutsideQuotedAndComments(sql, func(segment string) string {
		segment = withSystemVersioningRegex.ReplaceAllString(segment, " ")
		segment = withoutSystemVersioningRegex.ReplaceAllString(segment, " ")
		segment = periodSystemTimeRegex.ReplaceAllString(segment, " ")
		segment = rowStartRegex.ReplaceAllString(segment, " ")
		segment = rowEndRegex.ReplaceAllString(segment, " ")
		segment = alterSystemVersioningRegex.ReplaceAllString(segment, " ")
		return segment
	})
}

func hasColumnAttributes(sql string) bool {
	return MatchOutsideQuotedAndComments(sql, columnAttributeRegex.MatchString)
}

func stripColumnAttributes(sql string) string {
	return TransformOutsideQuotedAndComments(sql, func(segment string) string {
		return columnAttributeRegex.ReplaceAllString(segment, " ")
	})
}

func hasSequenceType(sql string) bool {
	return MatchOutsideQuotedAndComments(sql, func(segment string) bool {
		return sequenceTypeRegex.MatchString(segment) || alterSequenceTypeRegex.MatchString(segment)
	})
}

func stripSequenceType(sql string) string {
	return TransformOutsideQuotedAndComments(sql, func(segment string) string {
		segment = sequenceTypeRegex.ReplaceAllString(segment, "$1")
		segment = alterSequenceTypeRegex.ReplaceAllString(segment, "$1")
		return segment
	})
}

func hasCreateOrReplace(sql string) bool {
	return MatchOutsideQuotedAndComments(sql, func(segment string) bool {
		return createOrReplaceIndexRegex.MatchString(segment) ||
			createOrReplaceTableRegex.MatchString(segment) ||
			createOrReplaceSequenceRegex.MatchString(segment)
	})
}

func rewriteCreateOrReplace(sql string) string {
	return TransformOutsideQuotedAndComments(sql, func(segment string) string {
		segment = createOrReplaceIndexRegex.ReplaceAllString(segment, "DROP INDEX IF EXISTS $1 ON $2; CREATE INDEX $1 ON $2")
		segment = createOrReplaceTableRegex.ReplaceAllString(segment, "DROP TABLE IF EXISTS $1; CREATE TABLE $1")
		segment = createOrReplaceSequenceRegex.ReplaceAllString(segment, "DROP SEQUENCE IF EXISTS $1; CREATE SEQUENCE $1")
		return segment
	})
}

// TransformOutsideQuotedAndComments applies transform only to SQL code segments,
// skipping string literals and non-version comments.
func TransformOutsideQuotedAndComments(sql string, transform func(segment string) string) string {
	if transform == nil || sql == "" {
		return sql
	}

	segments := splitSQLSegments(sql)
	var out strings.Builder
	out.Grow(len(sql))
	for _, seg := range segments {
		if seg.isCode {
			out.WriteString(transform(seg.text))
			continue
		}
		out.WriteString(seg.text)
	}
	return out.String()
}

// MatchOutsideQuotedAndComments reports whether match returns true on any SQL
// code segment, skipping string literals and non-version comments.
func MatchOutsideQuotedAndComments(sql string, match func(segment string) bool) bool {
	if match == nil || sql == "" {
		return false
	}

	segments := splitSQLSegments(sql)
	for _, seg := range segments {
		if !seg.isCode {
			continue
		}
		if match(seg.text) {
			return true
		}
	}
	return false
}

func hasTrailingCommas(sql string) bool {
	_, changed := stripTrailingCommas(sql)
	return changed
}

func stripTrailingCommas(sql string) (string, bool) {
	var out strings.Builder
	out.Grow(len(sql))
	changed := false

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
			changed = true
			continue
		}

		out.WriteByte(ch)
	}

	return out.String(), changed
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

func splitSQLSegments(sql string) []sqlSegment {
	if len(sql) == 0 {
		return nil
	}

	segs := make([]sqlSegment, 0, 8)
	last := 0

	for i := 0; i < len(sql); i++ {
		var end int
		switch {
		case sql[i] == '\'':
			end = skipSingleQuotedString(sql, i)
		case sql[i] == '"':
			end = skipDoubleQuotedString(sql, i)
		case sql[i] == '`':
			end = skipBacktickQuotedIdentifier(sql, i)
		case isLineCommentStart(sql, i):
			end = skipLineComment(sql, i)
			if end < len(sql) {
				end++
			}
		case sql[i] == '#':
			end = skipLineComment(sql, i)
			if end < len(sql) {
				end++
			}
		case isBlockCommentStart(sql, i) && !isVersionMacroCommentStart(sql, i):
			end = skipBlockComment(sql, i)
		default:
			continue
		}

		if last < i {
			segs = append(segs, sqlSegment{text: sql[last:i], isCode: true})
		}
		if end > i {
			segs = append(segs, sqlSegment{text: sql[i:end], isCode: false})
			last = end
			i = end - 1
		}
	}

	if last < len(sql) {
		segs = append(segs, sqlSegment{text: sql[last:], isCode: true})
	}
	return segs
}

func isVersionMacroCommentStart(sql string, i int) bool {
	return i+2 < len(sql) && sql[i] == '/' && sql[i+1] == '*' && sql[i+2] == '!'
}

func skipSingleQuotedString(sql string, i int) int {
	i++
	for i < len(sql) {
		if sql[i] == '\\' && i+1 < len(sql) {
			i += 2
			continue
		}
		if sql[i] == '\'' {
			if i+1 < len(sql) && sql[i+1] == '\'' {
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return len(sql)
}

func skipDoubleQuotedString(sql string, i int) int {
	i++
	for i < len(sql) {
		if sql[i] == '\\' && i+1 < len(sql) {
			i += 2
			continue
		}
		if sql[i] == '"' {
			if i+1 < len(sql) && sql[i+1] == '"' {
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return len(sql)
}

func skipBacktickQuotedIdentifier(sql string, i int) int {
	i++
	for i < len(sql) {
		if sql[i] == '`' {
			if i+1 < len(sql) && sql[i+1] == '`' {
				i += 2
				continue
			}
			return i + 1
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
