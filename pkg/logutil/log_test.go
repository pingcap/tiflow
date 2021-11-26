// Copyright 2020 PingCAP, Inc.
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
	"bytes"
	"context"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestSuite(t *testing.T) {
	check.TestingT(t)
}

type logSuite struct{}

var _ = check.Suite(&logSuite{})

func (s *logSuite) TestInitLoggerAndSetLogLevel(c *check.C) {
	defer testleak.AfterTest(c)()
	f := filepath.Join(c.MkDir(), "test")
	cfg := &Config{
		Level: "warning",
		File:  f,
	}
	cfg.Adjust()
	err := InitLogger(cfg)
	c.Assert(err, check.IsNil)
	c.Assert(log.GetLevel(), check.Equals, zapcore.WarnLevel)

	// Set a different level.
	err = SetLogLevel("info")
	c.Assert(err, check.IsNil)
	c.Assert(log.GetLevel(), check.Equals, zapcore.InfoLevel)

	// Set the same level.
	err = SetLogLevel("info")
	c.Assert(err, check.IsNil)
	c.Assert(log.GetLevel(), check.Equals, zapcore.InfoLevel)

	// Set an invalid level.
	err = SetLogLevel("badlevel")
	c.Assert(err, check.NotNil)
}

func (s *logSuite) TestZapErrorFilter(c *check.C) {
	defer testleak.AfterTest(c)()
	var (
		err       = errors.New("test error")
		testCases = []struct {
			err      error
			filters  []error
			expected zap.Field
		}{
			{nil, []error{}, zap.Error(nil)},
			{err, []error{}, zap.Error(err)},
			{err, []error{context.Canceled}, zap.Error(err)},
			{err, []error{err}, zap.Error(nil)},
			{context.Canceled, []error{context.Canceled}, zap.Error(nil)},
			{errors.Annotate(context.Canceled, "annotate error"), []error{context.Canceled}, zap.Error(nil)},
		}
	)
	for _, tc := range testCases {
		c.Assert(ZapErrorFilter(tc.err, tc.filters...), check.DeepEquals, tc.expected)
	}
}

func getLinesCount(logFile string) (int, error) {
	f, err := os.Open(logFile)
	if err != nil {
		return 0, err
	}
	bytesContent, err := ioutil.ReadAll(f)
	if err != nil {
		return 0, err
	}
	sep := []byte{'\n'}
	cnt := bytes.Count(bytesContent, sep)

	return cnt, nil
}

func getRandomStr(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	r := make([]rune, n)
	for i := range r {
		r[i] = letters[rand.Intn(len(letters))]
	}
	return string(r)
}

func (s *logSuite) TestLogSampleAndDrop(c *check.C) {
	defer testleak.AfterTest(c)()
	f := filepath.Join(c.MkDir(), "test")
	cfg := &Config{
		Level:              "info",
		File:               f,
		SamplingInitial:    10,
		SamplingThereafter: 10,
	}
	err := InitLogger(cfg)
	c.Assert(err, check.IsNil)

	// info logs with the same message should be logged as [0~9,19,29,39,49,59,69,79,89,99]
	for i := 0; i < 100; i++ {
		log.Info("test ticdc log info", zap.Int("index", i))
	}
	cnt, err := getLinesCount(f)
	c.Assert(err, check.IsNil)
	c.Assert(cnt, check.Equals, 19)

	// clear the log file
	err = os.Truncate(f, 0)
	c.Assert(err, check.IsNil)

	// debug logs should not be logged
	for i := 0; i < 100; i++ {
		log.Debug("test ticdc log debug", zap.Int("index", i))
	}

	cnt, err = getLinesCount(f)
	c.Assert(err, check.IsNil)
	c.Assert(cnt, check.Equals, 0)

	// clear the log file
	err = os.Truncate(f, 0)
	c.Assert(err, check.IsNil)

	// all warn logs without the same message should be logged
	for i := 0; i < 100; i++ {
		log.Warn(getRandomStr(5), zap.Int("index", i))
	}
	cnt, err = getLinesCount(f)
	c.Assert(err, check.IsNil)
	c.Assert(cnt, check.Equals, 100)
}
