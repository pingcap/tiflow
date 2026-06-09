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
	"testing"
	"time"

	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/streamer"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/stretchr/testify/suite"
)

func TestPurgerSuite(t *testing.T) {
	suite.Run(t, new(testPurgerSuite))
}

type testPurgerSuite struct {
	suite.Suite
	uuids          []string
	relayFiles     [][]string
	activeRelayLog *streamer.RelayLogInfo
}

func (t *testPurgerSuite) SetupTest() {
	t.uuids = []string{
		"c6ae5afe-c7a3-11e8-a19d-0242ac130006.000001",
		"e9540a0d-f16d-11e8-8cb7-0242ac130008.000002",
		"195c342f-f46e-11e8-927c-0242ac150008.000003",
	}
	t.relayFiles = [][]string{
		{"mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000003"},
		{"mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000003"},
		{"mysql-bin.000001", "mysql-bin.000002", "mysql-bin.000003"},
	}
	t.activeRelayLog = &streamer.RelayLogInfo{
		TaskName:     fakeStrategyTaskName,
		SubDir:       "e9540a0d-f16d-11e8-8cb7-0242ac130008.000002",
		SubDirSuffix: 2,
		Filename:     "mysql-bin.000003", // last in second sub dir
	}
}

func (t *testPurgerSuite) EarliestActiveRelayLog() *streamer.RelayLogInfo {
	return t.activeRelayLog
}

func (t *testPurgerSuite) TestPurgeManuallyInactive() {
	// create relay log dir
	baseDir := t.T().TempDir()

	// prepare files and directories
	relayDirsPath, relayFilesPath, _ := t.genRelayLogFiles(baseDir, -1, -1)
	t.Require().Equal(3, len(relayDirsPath))
	t.Require().Equal(3, len(relayFilesPath))
	t.Require().Equal(3, len(relayFilesPath[2]))

	err := t.genUUIDIndexFile(baseDir)
	t.Require().NoError(err)

	cfg := config.PurgeConfig{
		Interval: 0, // disable automatically
	}

	purger := NewPurger(cfg, baseDir, []Operator{t}, nil)

	req := &pb.PurgeRelayRequest{
		Inactive: true,
	}
	err = purger.Do(context.Background(), req)
	t.Require().NoError(err)

	t.Require().False(utils.IsDirExists(relayDirsPath[0]))
	t.Require().True(utils.IsDirExists(relayDirsPath[1]))
	t.Require().True(utils.IsDirExists(relayDirsPath[2]))

	t.Require().False(utils.IsFileExists(relayFilesPath[1][0]))
	t.Require().False(utils.IsFileExists(relayFilesPath[1][1]))
	t.Require().True(utils.IsFileExists(relayFilesPath[1][2]))
	for _, fp := range relayFilesPath[2] {
		t.Require().True(utils.IsFileExists(fp))
	}
}

func (t *testPurgerSuite) TestPurgeManuallyTime() {
	// create relay log dir
	baseDir := t.T().TempDir()

	// prepare files and directories
	relayDirsPath, relayFilesPath, safeTime := t.genRelayLogFiles(baseDir, 1, 0)
	t.Require().Equal(3, len(relayDirsPath))
	t.Require().Equal(3, len(relayFilesPath))
	t.Require().Equal(3, len(relayFilesPath[2]))

	err := t.genUUIDIndexFile(baseDir)
	t.Require().NoError(err)

	cfg := config.PurgeConfig{
		Interval: 0, // disable automatically
	}

	purger := NewPurger(cfg, baseDir, []Operator{t}, nil)

	req := &pb.PurgeRelayRequest{
		Time: safeTime.Unix(),
	}
	err = purger.Do(context.Background(), req)
	t.Require().NoError(err)

	t.Require().False(utils.IsDirExists(relayDirsPath[0]))
	t.Require().True(utils.IsDirExists(relayDirsPath[1]))
	t.Require().True(utils.IsDirExists(relayDirsPath[2]))

	t.Require().False(utils.IsFileExists(relayFilesPath[1][0]))
	t.Require().True(utils.IsFileExists(relayFilesPath[1][1]))
	t.Require().True(utils.IsFileExists(relayFilesPath[1][2]))
	for _, fp := range relayFilesPath[2] {
		t.Require().True(utils.IsFileExists(fp))
	}
}

func (t *testPurgerSuite) TestPurgeManuallyFilename() {
	// create relay log dir
	baseDir := t.T().TempDir()

	// prepare files and directories
	relayDirsPath, relayFilesPath, _ := t.genRelayLogFiles(baseDir, -1, -1)
	t.Require().Equal(3, len(relayDirsPath))
	t.Require().Equal(3, len(relayFilesPath))
	t.Require().Equal(3, len(relayFilesPath[2]))

	err := t.genUUIDIndexFile(baseDir)
	t.Require().NoError(err)

	cfg := config.PurgeConfig{
		Interval: 0, // disable automatically
	}

	purger := NewPurger(cfg, baseDir, []Operator{t}, nil)

	req := &pb.PurgeRelayRequest{
		Filename: t.relayFiles[0][2],
		SubDir:   t.uuids[0],
	}
	err = purger.Do(context.Background(), req)
	t.Require().NoError(err)

	t.Require().True(utils.IsDirExists(relayDirsPath[0]))
	t.Require().True(utils.IsDirExists(relayDirsPath[1]))
	t.Require().True(utils.IsDirExists(relayDirsPath[2]))

	t.Require().False(utils.IsFileExists(relayFilesPath[0][0]))
	t.Require().False(utils.IsFileExists(relayFilesPath[0][1]))
	t.Require().True(utils.IsFileExists(relayFilesPath[0][2]))
	for _, fp := range relayFilesPath[1] {
		t.Require().True(utils.IsFileExists(fp))
	}
	for _, fp := range relayFilesPath[2] {
		t.Require().True(utils.IsFileExists(fp))
	}
}

func (t *testPurgerSuite) TestPurgeAutomaticallyTime() {
	// create relay log dir
	baseDir := t.T().TempDir()

	// prepare files and directories
	relayDirsPath, relayFilesPath, _ := t.genRelayLogFiles(baseDir, -1, -1)
	t.Require().Equal(3, len(relayDirsPath))
	t.Require().Equal(3, len(relayFilesPath))
	t.Require().Equal(3, len(relayFilesPath[2]))

	err := t.genUUIDIndexFile(baseDir)
	t.Require().NoError(err)

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
			t.Require().NoError(err)
		}
	}

	purger := NewPurger(cfg, baseDir, []Operator{t}, nil)
	purger.Start()
	time.Sleep(2 * time.Second) // sleep enough time to purge all inactive relay log files
	purger.Close()

	t.Require().False(utils.IsDirExists(relayDirsPath[0]))
	t.Require().True(utils.IsDirExists(relayDirsPath[1]))
	t.Require().True(utils.IsDirExists(relayDirsPath[2]))

	t.Require().False(utils.IsFileExists(relayFilesPath[1][0]))
	t.Require().False(utils.IsFileExists(relayFilesPath[1][1]))
	t.Require().True(utils.IsFileExists(relayFilesPath[1][2]))
	for _, fp := range relayFilesPath[2] {
		t.Require().True(utils.IsFileExists(fp))
	}
}

func (t *testPurgerSuite) TestPurgeAutomaticallySpace() {
	// create relay log dir
	baseDir := t.T().TempDir()

	// prepare files and directories
	relayDirsPath, relayFilesPath, _ := t.genRelayLogFiles(baseDir, -1, -1)
	t.Require().Equal(3, len(relayDirsPath))
	t.Require().Equal(3, len(relayFilesPath))
	t.Require().Equal(3, len(relayFilesPath[2]))

	err := t.genUUIDIndexFile(baseDir)
	t.Require().NoError(err)

	storageSize, err := utils.GetStorageSize(baseDir)
	t.Require().NoError(err)

	cfg := config.PurgeConfig{
		Interval:    1,                                                  // enable automatically
		RemainSpace: int64(storageSize.Available)/1024/1024/1024 + 1024, // always trigger purge
	}

	purger := NewPurger(cfg, baseDir, []Operator{t}, nil)
	purger.Start()
	time.Sleep(2 * time.Second) // sleep enough time to purge all inactive relay log files
	purger.Close()

	t.Require().False(utils.IsDirExists(relayDirsPath[0]))
	t.Require().True(utils.IsDirExists(relayDirsPath[1]))
	t.Require().True(utils.IsDirExists(relayDirsPath[2]))

	t.Require().False(utils.IsFileExists(relayFilesPath[1][0]))
	t.Require().False(utils.IsFileExists(relayFilesPath[1][1]))
	t.Require().True(utils.IsFileExists(relayFilesPath[1][2]))
	for _, fp := range relayFilesPath[2] {
		t.Require().True(utils.IsFileExists(fp))
	}
}

func (t *testPurgerSuite) genRelayLogFiles(baseDir string, safeTimeIdxI, safeTimeIdxJ int) ([]string, [][]string, time.Time) {
	var (
		relayDirsPath  = make([]string, 0, 3)
		relayFilesPath = make([][]string, 0, 3)
		safeTime       = time.Unix(0, 0)
	)

	for _, uuid := range t.uuids {
		dir := filepath.Join(baseDir, uuid)
		err := os.Mkdir(dir, 0o700)
		t.Require().NoError(err)
		relayDirsPath = append(relayDirsPath, dir)
	}

	// create relay log files
	for i, uuid := range t.uuids {
		dir := filepath.Join(baseDir, uuid)
		relayFilesPath = append(relayFilesPath, []string{})
		for j, fn := range t.relayFiles[i] {
			fp := filepath.Join(dir, fn)
			err2 := os.WriteFile(fp, []byte("meaningless file content"), 0o644)
			t.Require().Nil(err2)
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

func (t *testPurgerSuite) TestPurgerInterceptor() {
	cfg := config.PurgeConfig{}
	interceptor := newFakeInterceptor()

	purger := NewPurger(cfg, "", []Operator{t}, []PurgeInterceptor{interceptor})

	req := &pb.PurgeRelayRequest{
		Inactive: true,
	}
	err := purger.Do(context.Background(), req)
	t.Require().Error(err)
	t.Require().True(strings.Contains(err.Error(), interceptor.msg))
}
