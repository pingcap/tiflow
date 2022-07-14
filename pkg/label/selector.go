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

package label

import (
	"regexp"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Op represents the type of operator associated with a selector.
type Op string

const (
	// OpEq represents the Equals operator.
	OpEq = Op("eq")

	// OpNeq represents the Not Equal operator.
	OpNeq = Op("neq")

	// OpRegex represents the Regular Expression match operator.
	OpRegex = Op("regex")
)

// Selector is one selector on a label Set.
type Selector struct {
	// Key is the key of the label that could potentially match the selector.
	Key Key `json:"label"`
	// Target is the argument to the operator. In the case of the
	// Eq operator, it is the exact string that needs to match the value.
	Target string `json:"target"`
	// Op is the operator.
	Op Op `json:"op"`

	// regex stores a compiled Regular Expression.
	// It is not nil only if Op == OpRegex.
	regex atomic.Value // *regexp.Regexp
}

// Validate returns whether the selector is valid.
// Calling Matches on invalid selectors results in undefined behavior.
func (s *Selector) Validate() error {
	if err := checkLabelStrValid(string(s.Key)); err != nil {
		return errors.Annotate(err, "validate selector key")
	}

	if s.Op != OpEq && s.Op != OpNeq && s.Op != OpRegex {
		return errors.Errorf("invalid selector op: key: %s, op: %s", string(s.Key), string(s.Op))
	}

	if s.Op == OpRegex {
		_, err := s.getRegex()
		if err != nil {
			return errors.Annotatef(err, "validating selector key: %s", string(s.Key))
		}
	}
	return nil
}

// Matches returns whether the given selector matches
// the labelSet.
// Calling Matches on invalid selectors results in undefined behavior.
// Refer to Validate().
func (s *Selector) Matches(labelSet Set) bool {
	value, exists := labelSet.Get(s.Key)
	switch s.Op {
	case OpEq:
		if !exists {
			// OpEq should fail if the specified key
			// does not exist.
			return false
		}
		return s.Target == string(value)
	case OpNeq:
		if !exists {
			// OpNeq succeeds when the specified key
			// does not exist.
			return true
		}
		return s.Target != string(value)
	case OpRegex:
		if !exists {
			// OpRegex should fail if the specified key
			// does not exist.
			return false
		}
		regex, err := s.getRegex()
		if err != nil {
			log.Warn("illegal regular expression",
				zap.Any("selector", s))

			// We only make the selector not match,
			// because it only complicates things to force the caller to
			// handle such errors that are preventable with a pre-check.
			return false
		}

		return regex.MatchString(string(value))
	default:
	}

	panic("unreachable")
}

func (s *Selector) getRegex() (*regexp.Regexp, error) {
	if s.Op != OpRegex {
		panic("unreachable")
	}

	if s.regex.Load() == nil {
		regex, err := regexp.Compile(s.Target)
		if err != nil {
			// Handles error in case that the regular expression
			// is not legal.
			return nil, errors.Trace(err)
		}
		s.regex.CompareAndSwap(nil, regex)
	}

	return s.regex.Load().(*regexp.Regexp), nil
}
