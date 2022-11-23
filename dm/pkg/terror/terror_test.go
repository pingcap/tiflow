// Copyright 2019 PingCAP, Inc.
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

package terror

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/check"
	perrors "github.com/pingcap/errors"
)

func TestT(t *testing.T) {
	check.TestingT(t)
}

var _ = check.Suite(&testTErrorSuite{})

type testTErrorSuite struct{}

func (t *testTErrorSuite) TestTError(c *check.C) {
	var (
		code                  = codeDBBadConn
		class                 = ClassDatabase
		scope                 = ScopeUpstream
		level                 = LevelMedium
		message               = "bad connection"
		workaround            = "please check your network connection"
		messageArgs           = "message with args: %s"
		commonErr             = errors.New("common error")
		errFormat             = errBaseFormat + ", Message: %s, Workaround: %s"
		errFormatWithArg      = errBaseFormat + ", Message: %s: %s, Workaround: %s"
		errFormatWithRawCause = errBaseFormat + ", Message: %s, RawCause: %s, Workaround: %s"
	)

	c.Assert(ClassDatabase.String(), check.Equals, errClass2Str[ClassDatabase])
	c.Assert(ErrClass(10000).String(), check.Equals, "unknown error class: 10000")

	c.Assert(ScopeUpstream.String(), check.Equals, errScope2Str[ScopeUpstream])
	c.Assert(ErrScope(10000).String(), check.Equals, "unknown error scope: 10000")

	c.Assert(LevelHigh.String(), check.Equals, errLevel2Str[LevelHigh])
	c.Assert(ErrLevel(10000).String(), check.Equals, "unknown error level: 10000")

	// test Error basic API
	err := New(code, class, scope, level, message, workaround)
	c.Assert(err.Code(), check.Equals, code)
	c.Assert(err.Class(), check.Equals, class)
	c.Assert(err.Scope(), check.Equals, scope)
	c.Assert(err.Level(), check.Equals, level)
	c.Assert(err.Workaround(), check.Equals, workaround)
	c.Assert(err.Error(), check.Equals, fmt.Sprintf(errFormat, code, class, scope, level, err.getMsg(), workaround))

	setMsgErr := err.SetMessage(messageArgs)
	c.Assert(setMsgErr.getMsg(), check.Equals, messageArgs)
	setMsgErr.args = []interface{}{"1062"}
	c.Assert(setMsgErr.getMsg(), check.Equals, fmt.Sprintf(messageArgs, setMsgErr.args...))

	// test Error Generate/Generatef
	err2 := err.Generate("1063")
	c.Assert(err.Equal(err2), check.IsTrue)
	c.Assert(err2.Error(), check.Equals, fmt.Sprintf(errFormat, code, class, scope, level, "bad connection%!(EXTRA string=1063)", workaround))

	err3 := err.Generatef("new message format: %s", "1064")
	c.Assert(err.Equal(err3), check.IsTrue)
	c.Assert(err3.Error(), check.Equals, fmt.Sprintf(errFormatWithArg, code, class, scope, level, "new message format", "1064", workaround))

	// test Error Delegate
	c.Assert(err.Delegate(nil, "nil"), check.IsNil)
	err4 := err.Delegate(commonErr)
	c.Assert(err.Equal(err4), check.IsTrue)
	c.Assert(err4.Error(), check.Equals, fmt.Sprintf(errFormatWithRawCause, code, class, scope, level, message, commonErr, workaround))
	c.Assert(perrors.Cause(err4), check.Equals, commonErr)

	argsErr := New(code, class, scope, level, messageArgs, workaround)
	err4 = argsErr.Delegate(commonErr, "1065")
	c.Assert(argsErr.Equal(err4), check.IsTrue)
	c.Assert(err4.Error(), check.Equals, fmt.Sprintf(errFormatWithRawCause, code, class, scope, level, "message with args: 1065", commonErr, workaround))

	// test Error AnnotateDelegate
	c.Assert(err.AnnotateDelegate(nil, "message", "args"), check.IsNil)
	err5 := err.AnnotateDelegate(commonErr, "annotate delegate error: %d", 1066)
	c.Assert(err.Equal(err5), check.IsTrue)
	c.Assert(err5.Error(), check.Equals, fmt.Sprintf(errFormatWithRawCause, code, class, scope, level, "annotate delegate error: 1066", commonErr, workaround))

	// test Error Annotate
	oldMsg := err.getMsg()
	err6 := Annotate(err, "annotate error")
	c.Assert(err.Equal(err6), check.IsTrue)
	c.Assert(err6.Error(), check.Equals, fmt.Sprintf(errFormatWithArg, code, class, scope, level, "annotate error", oldMsg, workaround))

	c.Assert(Annotate(nil, ""), check.IsNil)
	annotateErr := Annotate(commonErr, "annotate")
	_, ok := annotateErr.(*Error)
	c.Assert(ok, check.IsFalse)
	c.Assert(perrors.Cause(annotateErr), check.Equals, commonErr)

	// test Error Annotatef
	oldMsg = err.getMsg()
	err7 := Annotatef(err, "annotatef error %s", "1067")
	c.Assert(err.Equal(err7), check.IsTrue)
	c.Assert(err7.Error(), check.Equals, fmt.Sprintf(errFormatWithArg, code, class, scope, level, "annotatef error 1067", oldMsg, workaround))

	c.Assert(Annotatef(nil, ""), check.IsNil)
	annotateErr = Annotatef(commonErr, "annotatef %s", "1068")
	_, ok = annotateErr.(*Error)
	c.Assert(ok, check.IsFalse)
	c.Assert(perrors.Cause(annotateErr), check.Equals, commonErr)

	// test format
	c.Assert(fmt.Sprintf("%q", err), check.Equals, fmt.Sprintf("%q", err.Error()))
	// err has no stack trace
	c.Assert(fmt.Sprintf("%+v", err), check.Equals, err.Error())
	c.Assert(fmt.Sprintf("%v", err), check.Equals, err.Error())
	// err2 has stack trace
	verbose := strings.Split(fmt.Sprintf("%+v", err2), "\n")
	c.Assert(len(verbose) > 5, check.IsTrue)
	c.Assert(verbose[0], check.Equals, err2.Error())
	c.Assert(verbose[1], check.Matches, ".*\\(\\*Error\\)\\.Generate")
	c.Assert(fmt.Sprintf("%v", err2), check.Equals, err2.Error())

	// test Message function
	c.Assert(Message(nil), check.Equals, "")
	c.Assert(Message(commonErr), check.Equals, commonErr.Error())
	c.Assert(Message(err), check.Equals, err.getMsg())
}

func (t *testTErrorSuite) TestTErrorStackTrace(c *check.C) {
	err := ErrDBUnExpect

	testCases := []struct {
		fn               string
		message          string
		args             []interface{}
		stackFingerprint string
	}{
		{"new", "new error", nil, ".*\\(\\*Error\\)\\.New"},
		{"generate", "", []interface{}{"parma1"}, ".*\\(\\*Error\\)\\.Generate"},
		{"generatef", "generatef error %s %d", []interface{}{"param1", 12}, ".*\\(\\*Error\\)\\.Generatef"},
	}

	for _, tc := range testCases {
		var err2 error
		switch tc.fn {
		case "new":
			err2 = err.New(tc.message)
		case "generate":
			err2 = err.Generate(tc.args...)
		case "generatef":
			err2 = err.Generatef(tc.message, tc.args...)
		}
		verbose := strings.Split(fmt.Sprintf("%+v", err2), "\n")
		c.Assert(len(verbose) > 5, check.IsTrue)
		c.Assert(verbose[0], check.Equals, err2.Error())
		c.Assert(verbose[1], check.Matches, tc.stackFingerprint)
	}
}

func (t *testTErrorSuite) TestTerrorWithOperate(c *check.C) {
	var (
		code             = codeDBBadConn
		class            = ClassDatabase
		scope            = ScopeUpstream
		level            = LevelMedium
		message          = "message with args: %s"
		workaround       = "please check your connection"
		err              = New(code, class, scope, level, message, workaround)
		arg              = "arg"
		commonErr        = perrors.New("common error")
		errFormatWithArg = errBaseFormat + ", Message: %s: %s, Workaround: %s"
	)

	// test WithScope
	newScope := ScopeDownstream
	c.Assert(WithScope(nil, newScope), check.IsNil)
	c.Assert(WithScope(commonErr, newScope).Error(), check.Equals, fmt.Sprintf("error scope: %s: common error", newScope))
	err1 := WithScope(err.Generate(arg), newScope)
	c.Assert(err.Equal(err1), check.IsTrue)
	c.Assert(err1.Error(), check.Equals, fmt.Sprintf(errFormatWithArg, code, class, newScope, level, "message with args", arg, workaround))

	// test WithClass
	newClass := ClassFunctional
	c.Assert(WithClass(nil, newClass), check.IsNil)
	c.Assert(WithClass(commonErr, newClass).Error(), check.Equals, fmt.Sprintf("error class: %s: common error", newClass))
	err2 := WithClass(err.Generate(arg), newClass)
	c.Assert(err.Equal(err2), check.IsTrue)
	c.Assert(err2.Error(), check.Equals, fmt.Sprintf(errFormatWithArg, code, newClass, scope, level, "message with args", arg, workaround))
}

func (t *testTErrorSuite) TestTerrorCodeMap(c *check.C) {
	err, ok := ErrorFromCode(codeDBDriverError)
	c.Assert(ok, check.IsTrue)
	c.Assert(ErrDBDriverError.Equal(err), check.IsTrue)

	_, ok = ErrorFromCode(1000)
	c.Assert(ok, check.IsFalse)
}
