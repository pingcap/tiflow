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

	perrors "github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

func TestTError(t *testing.T) {
	t.Parallel()

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

	require.Equal(t, errClass2Str[ClassDatabase], ClassDatabase.String())
	require.Equal(t, "unknown error class: 10000", ErrClass(10000).String())

	require.Equal(t, errScope2Str[ScopeUpstream], ScopeUpstream.String())
	require.Equal(t, "unknown error scope: 10000", ErrScope(10000).String())

	require.Equal(t, errLevel2Str[LevelHigh], LevelHigh.String())
	require.Equal(t, "unknown error level: 10000", ErrLevel(10000).String())

	// test Error basic API
	err := New(code, class, scope, level, message, workaround)
	require.Equal(t, code, err.code)
	require.Equal(t, class, err.Class())
	require.Equal(t, scope, err.scope)
	require.Equal(t, level, err.level)
	require.Equal(t, workaround, err.workaround)
	require.Equal(t, fmt.Sprintf(errFormat, code, class, scope, level, err.getMsg(), workaround), err.Error())

	setMsgErr := err.SetMessage(messageArgs)
	require.Equal(t, messageArgs, setMsgErr.getMsg())
	setMsgErr.args = []interface{}{"1062"}
	require.Equal(t, fmt.Sprintf(messageArgs, setMsgErr.args...), setMsgErr.getMsg())

	// test Error Generate/Generatef
	err2 := err.Generate("1063")
	require.True(t, err.Equal(err2))
	require.Equal(t, fmt.Sprintf(errFormat, code, class, scope, level, "bad connection%!(EXTRA string=1063)", workaround), err2.Error())

	err3 := err.Generatef("new message format: %s", "1064")
	require.True(t, err.Equal(err3))
	require.Equal(t, fmt.Sprintf(errFormatWithArg, code, class, scope, level, "new message format", "1064", workaround), err3.Error())

	// test Error Delegate
	require.Nil(t, err.Delegate(nil, "nil"))
	err4 := err.Delegate(commonErr)
	require.True(t, err.Equal(err4))
	require.Equal(t, fmt.Sprintf(errFormatWithRawCause, code, class, scope, level, message, commonErr, workaround), err4.Error())
	require.Equal(t, commonErr, perrors.Cause(err4))

	argsErr := New(code, class, scope, level, messageArgs, workaround)
	err4 = argsErr.Delegate(commonErr, "1065")
	require.True(t, argsErr.Equal(err4))
	require.Equal(t, fmt.Sprintf(errFormatWithRawCause, code, class, scope, level, "message with args: 1065", commonErr, workaround), err4.Error())

	// test Error AnnotateDelegate
	require.Nil(t, err.AnnotateDelegate(nil, "message", "args"))
	err5 := err.AnnotateDelegate(commonErr, "annotate delegate error: %d", 1066)
	require.True(t, err.Equal(err5))
	require.Equal(t, fmt.Sprintf(errFormatWithRawCause, code, class, scope, level, "annotate delegate error: 1066", commonErr, workaround), err5.Error())

	// test Error Annotate
	oldMsg := err.getMsg()
	err6 := Annotate(err, "annotate error")
	require.True(t, err.Equal(err6))
	require.Equal(t, fmt.Sprintf(errFormatWithArg, code, class, scope, level, "annotate error", oldMsg, workaround), err6.Error())

	require.Nil(t, Annotate(nil, ""))
	annotateErr := Annotate(commonErr, "annotate")
	_, ok := annotateErr.(*Error)
	require.False(t, ok)
	require.Equal(t, commonErr, perrors.Cause(annotateErr))

	// test Error Annotatef
	oldMsg = err.getMsg()
	err7 := Annotatef(err, "annotatef error %s", "1067")
	require.True(t, err.Equal(err7))
	require.Equal(t, fmt.Sprintf(errFormatWithArg, code, class, scope, level, "annotatef error 1067", oldMsg, workaround), err7.Error())

	require.Nil(t, Annotatef(nil, ""))
	annotateErr = Annotatef(commonErr, "annotatef %s", "1068")
	_, ok = annotateErr.(*Error)
	require.False(t, ok)
	require.Equal(t, commonErr, perrors.Cause(annotateErr))

	// test format
	require.Equal(t, fmt.Sprintf("%q", err.Error()), fmt.Sprintf("%q", err))
	// err has no stack trace
	require.Equal(t, err.Error(), fmt.Sprintf("%+v", err))
	require.Equal(t, err.Error(), fmt.Sprintf("%v", err))

	// err2 has stack trace
	verbose := strings.Split(fmt.Sprintf("%+v", err2), "\n")
	require.True(t, len(verbose) > 5)
	require.Equal(t, err2.Error(), verbose[0])
	require.Regexp(t, ".*\\(\\*Error\\)\\.Generate", verbose[1])
	require.Equal(t, err2.Error(), fmt.Sprintf("%v", err2))

	// test Message function
	require.Equal(t, "", Message(nil))
	require.Equal(t, commonErr.Error(), Message(commonErr))
	require.Equal(t, err.getMsg(), Message(err))
}

func TestTErrorStackTrace(t *testing.T) {
	t.Parallel()

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
		require.True(t, len(verbose) > 5)
		require.Equal(t, err2.Error(), verbose[0])
		require.Regexp(t, tc.stackFingerprint, verbose[1])
	}
}

func TestTerrorWithOperate(t *testing.T) {
	t.Parallel()

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
	require.Nil(t, WithScope(nil, newScope))
	require.Equal(t, fmt.Sprintf("error scope: %s: common error", newScope), WithScope(commonErr, newScope).Error())
	err1 := WithScope(err.Generate(arg), newScope)
	require.True(t, err.Equal(err1))
	require.Equal(t, fmt.Sprintf(errFormatWithArg, code, class, newScope, level, "message with args", arg, workaround), err1.Error())

	// test WithClass
	newClass := ClassFunctional
	require.Nil(t, WithClass(nil, newClass))
	require.Equal(t, fmt.Sprintf("error class: %s: common error", newClass), WithClass(commonErr, newClass).Error())
	err2 := WithClass(err.Generate(arg), newClass)
	require.True(t, err.Equal(err2))
	require.Equal(t, fmt.Sprintf(errFormatWithArg, code, newClass, scope, level, "message with args", arg, workaround), err2.Error())
}

func TestTerrorCodeMap(t *testing.T) {
	t.Parallel()

	err, ok := ErrorFromCode(codeDBDriverError)
	require.True(t, ok)
	require.True(t, ErrDBDriverError.Equal(err))

	_, ok = ErrorFromCode(1000)
	require.False(t, ok)
}
