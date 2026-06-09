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
	"os"
	"path/filepath"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/streamer"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

func (t *testPurgerSuite) TestPurgeRelayFilesBeforeFile() {
	// UUID mismatch
	safeRelay := &streamer.RelayLogInfo{
		SubDir: "not-found-uuid",
	}
	files, err := getRelayFilesBeforeFile(log.L(), "", t.uuids, safeRelay)
	t.Require().Error(err)
	t.Require().Nil(files)

	// create relay log dir
	baseDir := t.T().TempDir()
	// empty relay log dirs
	safeRelay = &streamer.RelayLogInfo{
		SubDir: t.uuids[len(t.uuids)-1],
	}
	files, err = getRelayFilesBeforeFile(log.L(), baseDir, t.uuids, safeRelay)
	t.Require().Error(err)
	t.Require().Nil(files)

	// create relay log files
	relayDirsPath, relayFilesPath, _ := t.genRelayLogFiles(baseDir, -1, -1)

	// no older
	safeRelay = &streamer.RelayLogInfo{
		SubDir:   t.uuids[0],
		Filename: t.relayFiles[0][0],
	}
	files, err = getRelayFilesBeforeFile(log.L(), baseDir, t.uuids, safeRelay)
	t.Require().NoError(err)
	t.Require().Equal([]*subRelayFiles{}, files)

	// only relay files in first sub dir
	safeRelay = &streamer.RelayLogInfo{
		SubDir:   t.uuids[1],
		Filename: t.relayFiles[1][0],
	}
	files, err = getRelayFilesBeforeFile(log.L(), baseDir, t.uuids, safeRelay)
	t.Require().NoError(err)
	t.Require().Equal(1, len(files))
	t.Require().Equal(relayDirsPath[0], files[0].dir)
	t.Require().Equal(relayFilesPath[0], files[0].files)
	t.Require().True(files[0].hasAll)

	// relay files in first sub dir, and some in second sub dir
	safeRelay = &streamer.RelayLogInfo{
		SubDir:   t.uuids[1],
		Filename: t.relayFiles[1][len(t.relayFiles[1])-1],
	}
	files, err = getRelayFilesBeforeFile(log.L(), baseDir, t.uuids, safeRelay)
	t.Require().NoError(err)
	t.Require().Equal(2, len(files))
	t.Require().Equal(relayDirsPath[0], files[0].dir)
	t.Require().Equal(relayFilesPath[0], files[0].files)
	t.Require().True(files[0].hasAll)
	t.Require().Equal(relayDirsPath[1], files[1].dir)
	t.Require().Equal(relayFilesPath[1][:len(t.relayFiles[1])-1], files[1].files)
	t.Require().False(files[1].hasAll)

	// relay files in first and second sub dir, and some in third sub dir
	safeRelay = &streamer.RelayLogInfo{
		SubDir:   t.uuids[2],
		Filename: t.relayFiles[2][1],
	}
	files, err = getRelayFilesBeforeFile(log.L(), baseDir, t.uuids, safeRelay)
	t.Require().NoError(err)
	t.Require().Equal(3, len(files))
	t.Require().Equal(relayDirsPath[0], files[0].dir)
	t.Require().Equal(relayFilesPath[0], files[0].files)
	t.Require().True(files[0].hasAll)
	t.Require().Equal(relayDirsPath[1], files[1].dir)
	t.Require().Equal(relayFilesPath[1], files[1].files)
	t.Require().True(files[1].hasAll)
	t.Require().Equal(relayDirsPath[2], files[2].dir)
	t.Require().Equal(relayFilesPath[2][:1], files[2].files)
	t.Require().False(files[2].hasAll)

	// write a fake meta file to first sub dir
	fakeMeta := filepath.Join(relayDirsPath[0], utils.MetaFilename)
	t.Require().Nil(os.WriteFile(fakeMeta, []byte{}, 0o666))

	// purge all relay log files in first and second sub dir, and some in third sub dir
	err = purgeRelayFilesBeforeFile(log.L(), baseDir, t.uuids, safeRelay)
	t.Require().NoError(err)
	t.Require().False(utils.IsDirExists(relayDirsPath[0]))
	t.Require().False(utils.IsDirExists(relayDirsPath[1]))
	t.Require().True(utils.IsDirExists(relayDirsPath[2]))
	t.Require().False(utils.IsFileExists(relayFilesPath[2][0]))
	t.Require().True(utils.IsFileExists(relayFilesPath[2][1]))
	t.Require().True(utils.IsFileExists(relayFilesPath[2][2]))
}

func (t *testPurgerSuite) TestPurgeRelayFilesBeforeFileAndTime() {
	// create relay log dir
	baseDir := t.T().TempDir()

	// empty relay log dirs
	safeRelay := &streamer.RelayLogInfo{
		SubDir: t.uuids[len(t.uuids)-1],
	}
	files, err := getRelayFilesBeforeFileAndTime(log.L(), baseDir, t.uuids, safeRelay, time.Now())
	t.Require().Error(err)
	t.Require().Nil(files)

	// create relay log files
	relayDirsPath, relayFilesPath, safeTime := t.genRelayLogFiles(baseDir, 1, 1)

	// safeRelay older than safeTime
	safeRelay = &streamer.RelayLogInfo{
		SubDir:   t.uuids[1],
		Filename: t.relayFiles[1][0],
	}
	files, err = getRelayFilesBeforeFileAndTime(log.L(), baseDir, t.uuids, safeRelay, safeTime)
	t.Require().NoError(err)
	t.Require().Equal(1, len(files))
	t.Require().Equal(relayDirsPath[0], files[0].dir)
	t.Require().Equal(relayFilesPath[0], files[0].files)
	t.Require().True(files[0].hasAll)

	// safeRelay newer than safeTime
	safeRelay = &streamer.RelayLogInfo{
		SubDir:   t.uuids[2],
		Filename: t.relayFiles[2][0],
	}
	files, err = getRelayFilesBeforeFileAndTime(log.L(), baseDir, t.uuids, safeRelay, safeTime)
	t.Require().NoError(err)
	t.Require().Equal(2, len(files))
	t.Require().Equal(relayDirsPath[0], files[0].dir)
	t.Require().Equal(relayFilesPath[0], files[0].files)
	t.Require().True(files[0].hasAll)
	t.Require().Equal(relayDirsPath[1], files[1].dir)
	t.Require().Equal(relayFilesPath[1][:2], files[1].files)
	t.Require().False(files[1].hasAll)

	// write a fake meta file to first sub dir
	fakeMeta := filepath.Join(relayDirsPath[0], utils.MetaFilename)
	t.Require().Nil(os.WriteFile(fakeMeta, []byte{}, 0o666))

	// purge all relay log files in first and second sub dir, and some in third sub dir
	err = purgeRelayFilesBeforeFileAndTime(log.L(), baseDir, t.uuids, safeRelay, safeTime)
	t.Require().NoError(err)
	t.Require().False(utils.IsDirExists(relayDirsPath[0]))
	t.Require().True(utils.IsDirExists(relayDirsPath[1]))
	t.Require().True(utils.IsDirExists(relayDirsPath[2]))
	t.Require().False(utils.IsFileExists(relayFilesPath[1][0]))
	t.Require().False(utils.IsFileExists(relayFilesPath[1][1]))
	t.Require().True(utils.IsFileExists(relayFilesPath[1][2]))
}
