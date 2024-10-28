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

package relay

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/streamer"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

var _ = check.Suite(&testPurgerSuite{
	uuids: []string{
		"c6ae5afe-c7a3-11e8-a19d-0242ac130006.000001",
		"e9540a0d-f16d-11e8-8cb7-0242ac130008.000002",
		"195c342f-f46e-11e8-927c-0242ac150008.000003",
	},
	relayFiles: [][]string{
		{"mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000003"},
		{"mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000003"},
		{"mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000003"},
	},
	activeRelayLog: &streamer.RelayLogInfo{
		TaskName:     fakeStrategyTaskName,
		SubDir:       "e9540a0d-f16d-11e8-8cb7-0242ac130008.000002",
		SubDirSuffix: 2,
		Filename:     "mysql-bin.000003", // last in second sub dir
	},
})

type testPurgerSuite struct {
	uuids          []string
	relayFiles     [][]string
	activeRelayLog *streamer.RelayLogInfo
}

func (t *testPurgerSuite) EarliestActiveRelayLog() *streamer.RelayLogInfo {
	return t.activeRelayLog
}

func (t *testPurgerSuite) TestPurgeManuallyInactive(c *check.C) {
	// create relay log dir
	baseDir := c.MkDir()

	// prepare files and directories
	relayDirsPath, relayFilesPath, _ := t.genRelayLogFiles(c, baseDir, -1, -1)
	c.Assert(len(relayDirsPath), check.Equals, 3)
	c.Assert(len(relayFilesPath), check.Equals, 3)
	c.Assert(len(relayFilesPath[2]), check.Equals, 3)

	err := t.genUUIDIndexFile(baseDir)
	c.Assert(err, check.IsNil)

	cfg := config.PurgeConfig{
		Interval: 0, // disable automatically
	}

	purger := NewPurger(cfg, baseDir, []Operator{t}, nil)

	req := &pb.PurgeRelayRequest{
		Inactive: true,
	}
	err = purger.Do(context.Background(), req)
	c.Assert(err, check.IsNil)

	c.Assert(utils.IsDirExists(relayDirsPath[0]), check.IsFalse)
	c.Assert(utils.IsDirExists(relayDirsPath[1]), check.IsTrue)
	c.Assert(utils.IsDirExists(relayDirsPath[2]), check.IsTrue)

	c.Assert(utils.IsFileExists(relayFilesPath[1][0]), check.IsFalse)
	c.Assert(utils.IsFileExists(relayFilesPath[1][1]), check.IsFalse)
	c.Assert(utils.IsFileExists(relayFilesPath[1][2]), check.IsTrue)
	for _, fp := range relayFilesPath[2] {
		c.Assert(utils.IsFileExists(fp), check.IsTrue)
	}
}

func (t *testPurgerSuite) TestPurgeManuallyTime(c *check.C) {
	// create relay log dir
	baseDir := c.MkDir()

	// prepare files and directories
	relayDirsPath, relayFilesPath, safeTime := t.genRelayLogFiles(c, baseDir, 1, 0)
	c.Assert(len(relayDirsPath), check.Equals, 3)
	c.Assert(len(relayFilesPath), check.Equals, 3)
	c.Assert(len(relayFilesPath[2]), check.Equals, 3)

	err := t.genUUIDIndexFile(baseDir)
	c.Assert(err, check.IsNil)

	cfg := config.PurgeConfig{
		Interval: 0, // disable automatically
	}

	purger := NewPurger(cfg, baseDir, []Operator{t}, nil)

	req := &pb.PurgeRelayRequest{
		Time: safeTime.Unix(),
	}
	err = purger.Do(context.Background(), req)
	c.Assert(err, check.IsNil)

	c.Assert(utils.IsDirExists(relayDirsPath[0]), check.IsFalse)
	c.Assert(utils.IsDirExists(relayDirsPath[1]), check.IsTrue)
	c.Assert(utils.IsDirExists(relayDirsPath[2]), check.IsTrue)

	c.Assert(utils.IsFileExists(relayFilesPath[1][0]), check.IsFalse)
	c.Assert(utils.IsFileExists(relayFilesPath[1][1]), check.IsTrue)
	c.Assert(utils.IsFileExists(relayFilesPath[1][2]), check.IsTrue)
	for _, fp := range relayFilesPath[2] {
		c.Assert(utils.IsFileExists(fp), check.IsTrue)
	}
}

func (t *testPurgerSuite) TestPurgeManuallyFilename(c *check.C) {
	// create relay log dir
	baseDir := c.MkDir()

	// prepare files and directories
	relayDirsPath, relayFilesPath, _ := t.genRelayLogFiles(c, baseDir, -1, -1)
	c.Assert(len(relayDirsPath), check.Equals, 3)
	c.Assert(len(relayFilesPath), check.Equals, 3)
	c.Assert(len(relayFilesPath[2]), check.Equals, 3)

	err := t.genUUIDIndexFile(baseDir)
	c.Assert(err, check.IsNil)

	cfg := config.PurgeConfig{
		Interval: 0, // disable automatically
	}

	purger := NewPurger(cfg, baseDir, []Operator{t}, nil)

	req := &pb.PurgeRelayRequest{
		Filename: t.relayFiles[0][2],
		SubDir:   t.uuids[0],
	}
	err = purger.Do(context.Background(), req)
	c.Assert(err, check.IsNil)

	c.Assert(utils.IsDirExists(relayDirsPath[0]), check.IsTrue)
	c.Assert(utils.IsDirExists(relayDirsPath[1]), check.IsTrue)
	c.Assert(utils.IsDirExists(relayDirsPath[2]), check.IsTrue)

	c.Assert(utils.IsFileExists(relayFilesPath[0][0]), check.IsFalse)
	c.Assert(utils.IsFileExists(relayFilesPath[0][1]), check.IsFalse)
	c.Assert(utils.IsFileExists(relayFilesPath[0][2]), check.IsTrue)
	for _, fp := range relayFilesPath[1] {
		c.Assert(utils.IsFileExists(fp), check.IsTrue)
	}
	for _, fp := range relayFilesPath[2] {
		c.Assert(utils.IsFileExists(fp), check.IsTrue)
	}
}

func (t *testPurgerSuite) TestPurgeAutomaticallyTime(c *check.C) {
	// create relay log dir
	baseDir := c.MkDir()

	// prepare files and directories
	relayDirsPath, relayFilesPath, _ := t.genRelayLogFiles(c, baseDir, -1, -1)
	c.Assert(len(relayDirsPath), check.Equals, 3)
	c.Assert(len(relayFilesPath), check.Equals, 3)
	c.Assert(len(relayFilesPath[2]), check.Equals, 3)

	err := t.genUUIDIndexFile(baseDir)
	c.Assert(err, check.IsNil)

	cfg := config.PurgeConfig{
		Interval: 1, // enable automatically
		Expires:  1,
	}

	// change files' modification time
	aTime := time.Now().Add(time.Duration(-cfg.Expires*3) * time.Hour)
	mTime := time.Now().Add(time.Duration(-cfg.Expires*2) * time.Hour)
	for _, fps := range relayFilesPath {
		for _, fp := range fps {
			err = os.Chtimes(fp, aTime, mTime)
			c.Assert(err, check.IsNil)
		}
	}

	purger := NewPurger(cfg, baseDir, []Operator{t}, nil)
	purger.Start()
	time.Sleep(2 * time.Second) // sleep enough time to purge all inactive relay log files
	purger.Close()

	c.Assert(utils.IsDirExists(relayDirsPath[0]), check.IsFalse)
	c.Assert(utils.IsDirExists(relayDirsPath[1]), check.IsTrue)
	c.Assert(utils.IsDirExists(relayDirsPath[2]), check.IsTrue)

	c.Assert(utils.IsFileExists(relayFilesPath[1][0]), check.IsFalse)
	c.Assert(utils.IsFileExists(relayFilesPath[1][1]), check.IsFalse)
	c.Assert(utils.IsFileExists(relayFilesPath[1][2]), check.IsTrue)
	for _, fp := range relayFilesPath[2] {
		c.Assert(utils.IsFileExists(fp), check.IsTrue)
	}
}

func (t *testPurgerSuite) TestPurgeAutomaticallySpace(c *check.C) {
	// create relay log dir
	baseDir := c.MkDir()

	// prepare files and directories
	relayDirsPath, relayFilesPath, _ := t.genRelayLogFiles(c, baseDir, -1, -1)
	c.Assert(len(relayDirsPath), check.Equals, 3)
	c.Assert(len(relayFilesPath), check.Equals, 3)
	c.Assert(len(relayFilesPath[2]), check.Equals, 3)

	err := t.genUUIDIndexFile(baseDir)
	c.Assert(err, check.IsNil)

	storageSize, err := utils.GetStorageSize(baseDir)
	c.Assert(err, check.IsNil)

	cfg := config.PurgeConfig{
		Interval:    1,                                                  // enable automatically
		RemainSpace: int64(storageSize.Available)/1024/1024/1024 + 1024, // always trigger purge
	}

	purger := NewPurger(cfg, baseDir, []Operator{t}, nil)
	purger.Start()
	time.Sleep(2 * time.Second) // sleep enough time to purge all inactive relay log files
	purger.Close()

	c.Assert(utils.IsDirExists(relayDirsPath[0]), check.IsFalse)
	c.Assert(utils.IsDirExists(relayDirsPath[1]), check.IsTrue)
	c.Assert(utils.IsDirExists(relayDirsPath[2]), check.IsTrue)

	c.Assert(utils.IsFileExists(relayFilesPath[1][0]), check.IsFalse)
	c.Assert(utils.IsFileExists(relayFilesPath[1][1]), check.IsFalse)
	c.Assert(utils.IsFileExists(relayFilesPath[1][2]), check.IsTrue)
	for _, fp := range relayFilesPath[2] {
		c.Assert(utils.IsFileExists(fp), check.IsTrue)
	}
}

func (t *testPurgerSuite) genRelayLogFiles(c *check.C, baseDir string, safeTimeIdxI, safeTimeIdxJ int) ([]string, [][]string, time.Time) {
	var (
		relayDirsPath  = make([]string, 0, 3)
		relayFilesPath = make([][]string, 0, 3)
		safeTime       = time.Unix(0, 0)
	)

	for _, uuid := range t.uuids {
		dir := filepath.Join(baseDir, uuid)
		err := os.Mkdir(dir, 0o700)
		c.Assert(err, check.IsNil)
		relayDirsPath = append(relayDirsPath, dir)
	}

	// create relay log files
	for i, uuid := range t.uuids {
		dir := filepath.Join(baseDir, uuid)
		relayFilesPath = append(relayFilesPath, []string{})
		for j, fn := range t.relayFiles[i] {
			fp := filepath.Join(dir, fn)
			err2 := os.WriteFile(fp, []byte("meaningless file content"), 0o644)
			c.Assert(err2, check.IsNil)
			relayFilesPath[i] = append(relayFilesPath[i], fp)

			if i == safeTimeIdxI && j == safeTimeIdxJ {
				time.Sleep(time.Second)
				safeTime = time.Now()
				time.Sleep(time.Second)
			}
		}
	}

	return relayDirsPath, relayFilesPath, safeTime
}

func (t *testPurgerSuite) genUUIDIndexFile(baseDir string) error {
	fp := filepath.Join(baseDir, utils.UUIDIndexFilename)

	var buf bytes.Buffer
	for _, uuid := range t.uuids {
		buf.WriteString(uuid)
		buf.WriteString("\n")
	}

	return utils.WriteFileAtomic(fp, buf.Bytes(), 0o644)
}

type fakeInterceptor struct {
	msg string
}

func newFakeInterceptor() *fakeInterceptor {
	return &fakeInterceptor{
		msg: "forbid purge by fake interceptor",
	}
}

func (i *fakeInterceptor) ForbidPurge() (bool, string) {
	return true, i.msg
}

func (t *testPurgerSuite) TestPurgerInterceptor(c *check.C) {
	cfg := config.PurgeConfig{}
	interceptor := newFakeInterceptor()

	purger := NewPurger(cfg, "", []Operator{t}, []PurgeInterceptor{interceptor})

	req := &pb.PurgeRelayRequest{
		Inactive: true,
	}
	err := purger.Do(context.Background(), req)
	c.Assert(err, check.NotNil)
	c.Assert(strings.Contains(err.Error(), interceptor.msg), check.IsTrue)
}
