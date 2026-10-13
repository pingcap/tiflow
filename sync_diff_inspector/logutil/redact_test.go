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
// See the License for the specific language governing permissions and
// limitations under the License.

package logutil

import (
	"fmt"
	"testing"

	"github.com/pingcap/errors"
	dmlog "github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

func makeTestLogger() (*zap.Logger, *zaptest.Buffer) {
	buffer := new(zaptest.Buffer)
	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(zapcore.EncoderConfig{
			LevelKey:       "$lvl",
			MessageKey:     "$msg",
			EncodeLevel:    zapcore.CapitalLevelEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
		}),
		buffer,
		zap.DebugLevel,
	))
	return logger, buffer
}

type testRedactStringer struct {
	Name string
}

func (s testRedactStringer) String() string {
	return "stringer:" + s.Name
}

func TestZapRedactFields(t *testing.T) {
	original := dmlog.IsRedactLogEnabled()
	t.Cleanup(func() { dmlog.SetRedactLog(original) })

	logger, buffer := makeTestLogger()
	value := testRedactStringer{Name: "secret"}
	emit := func() {
		logger.Info("redact",
			ZapRedactReflect("reflect", value),
			ZapRedactStringer("stringer", value),
			ZapRedactStrings("strings", []string{"secret1", "secret2"}))
	}

	dmlog.SetRedactLog(false)
	emit()
	require.Equal(t, `{"$lvl":"INFO","$msg":"redact","reflect":{"Name":"secret"},`+
		`"stringer":"stringer:secret","strings":["secret1","secret2"]}`, buffer.Stripped())
	buffer.Reset()

	dmlog.SetRedactLog(true)
	emit()
	require.Equal(t, `{"$lvl":"INFO","$msg":"redact","reflect":"?","stringer":"?","strings":"?"}`,
		buffer.Stripped())
}

func TestZapRedactError(t *testing.T) {
	original := dmlog.IsRedactLogEnabled()
	t.Cleanup(func() { dmlog.SetRedactLog(original) })

	err := errors.Annotate(errors.New("sensitive-value"), "query failed")
	for _, redact := range []bool{false, true} {
		t.Run(fmt.Sprintf("redact=%t", redact), func(t *testing.T) {
			dmlog.SetRedactLog(redact)
			logger, buffer := makeTestLogger()
			logger.Warn("query failed", ZapRedactError(err))

			if redact {
				require.Equal(t, `{"$lvl":"WARN","$msg":"query failed","error":"?"}`, buffer.Stripped())
			} else {
				actual := buffer.Stripped()
				buffer.Reset()
				logger.Warn("query failed", zap.Error(err))
				require.Equal(t, buffer.Stripped(), actual)
				require.Contains(t, actual, "sensitive-value")
				require.Contains(t, actual, "errorVerbose")
			}

			buffer.Reset()
			logger.Warn("no error", ZapRedactError(nil))
			require.Equal(t, `{"$lvl":"WARN","$msg":"no error"}`, buffer.Stripped())
		})
	}
}
