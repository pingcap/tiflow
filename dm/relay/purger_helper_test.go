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

	. "github.com/pingcap/check"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/streamer"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

func (t *testPurgerSuite) TestPurgeRelayFilesBeforeFile(c *C) {
	// UUID mismatch
	safeRelay := &streamer.RelayLogInfo{
		SubDir: "not-found-uuid",
	}
	files, err := getRelayFilesBeforeFile(log.L(), "", t.uuids, safeRelay)
	c.Assert(err, NotNil)
	c.Assert(files, IsNil)

	// create relay log dir
	baseDir := c.MkDir()
	// empty relay log dirs
	safeRelay = &streamer.RelayLogInfo{
		SubDir: t.uuids[len(t.uuids)-1],
	}
	files, err = getRelayFilesBeforeFile(log.L(), baseDir, t.uuids, safeRelay)
	c.Assert(err, NotNil)
	c.Assert(files, IsNil)

	// create relay log files
	relayDirsPath, relayFilesPath, _ := t.genRelayLogFiles(c, baseDir, -1, -1)

	// no older
	safeRelay = &streamer.RelayLogInfo{
		SubDir:   t.uuids[0],
		Filename: t.relayFiles[0][0],
	}
	files, err = getRelayFilesBeforeFile(log.L(), baseDir, t.uuids, safeRelay)
	c.Assert(err, IsNil)
	c.Assert(files, DeepEquals, []*subRelayFiles{})

	// only relay files in first sub dir
	safeRelay = &streamer.RelayLogInfo{
		SubDir:   t.uuids[1],
		Filename: t.relayFiles[1][0],
	}
	files, err = getRelayFilesBeforeFile(log.L(), baseDir, t.uuids, safeRelay)
	c.Assert(err, IsNil)
	c.Assert(len(files), Equals, 1)
	c.Assert(files[0].dir, Equals, relayDirsPath[0])
	c.Assert(files[0].files, DeepEquals, relayFilesPath[0])
	c.Assert(files[0].hasAll, IsTrue)

	// relay files in first sub dir, and some in second sub dir
	safeRelay = &streamer.RelayLogInfo{
		SubDir:   t.uuids[1],
		Filename: t.relayFiles[1][len(t.relayFiles[1])-1],
	}
	files, err = getRelayFilesBeforeFile(log.L(), baseDir, t.uuids, safeRelay)
	c.Assert(err, IsNil)
	c.Assert(len(files), Equals, 2)
	c.Assert(files[0].dir, Equals, relayDirsPath[0])
	c.Assert(files[0].files, DeepEquals, relayFilesPath[0])
	c.Assert(files[0].hasAll, IsTrue)
	c.Assert(files[1].dir, Equals, relayDirsPath[1])
	c.Assert(files[1].files, DeepEquals, relayFilesPath[1][:len(t.relayFiles[1])-1])
	c.Assert(files[1].hasAll, IsFalse)

	// relay files in first and second sub dir, and some in third sub dir
	safeRelay = &streamer.RelayLogInfo{
		SubDir:   t.uuids[2],
		Filename: t.relayFiles[2][1],
	}
	files, err = getRelayFilesBeforeFile(log.L(), baseDir, t.uuids, safeRelay)
	c.Assert(err, IsNil)
	c.Assert(len(files), Equals, 3)
	c.Assert(files[0].dir, Equals, relayDirsPath[0])
	c.Assert(files[0].files, DeepEquals, relayFilesPath[0])
	c.Assert(files[0].hasAll, IsTrue)
	c.Assert(files[1].dir, Equals, relayDirsPath[1])
	c.Assert(files[1].files, DeepEquals, relayFilesPath[1])
	c.Assert(files[1].hasAll, IsTrue)
	c.Assert(files[2].dir, Equals, relayDirsPath[2])
	c.Assert(files[2].files, DeepEquals, relayFilesPath[2][:1])
	c.Assert(files[2].hasAll, IsFalse)

	// write a fake meta file to first sub dir
	fakeMeta := filepath.Join(relayDirsPath[0], utils.MetaFilename)
	c.Assert(os.WriteFile(fakeMeta, []byte{}, 0o666), IsNil)

	// purge all relay log files in first and second sub dir, and some in third sub dir
	err = purgeRelayFilesBeforeFile(log.L(), baseDir, t.uuids, safeRelay)
	c.Assert(err, IsNil)
	c.Assert(utils.IsDirExists(relayDirsPath[0]), IsFalse)
	c.Assert(utils.IsDirExists(relayDirsPath[1]), IsFalse)
	c.Assert(utils.IsDirExists(relayDirsPath[2]), IsTrue)
	c.Assert(utils.IsFileExists(relayFilesPath[2][0]), IsFalse)
	c.Assert(utils.IsFileExists(relayFilesPath[2][1]), IsTrue)
	c.Assert(utils.IsFileExists(relayFilesPath[2][2]), IsTrue)
}

func (t *testPurgerSuite) TestPurgeRelayFilesBeforeFileAndTime(c *C) {
	// create relay log dir
	baseDir := c.MkDir()

	// empty relay log dirs
	safeRelay := &streamer.RelayLogInfo{
		SubDir: t.uuids[len(t.uuids)-1],
	}
	files, err := getRelayFilesBeforeFileAndTime(log.L(), baseDir, t.uuids, safeRelay, time.Now())
	c.Assert(err, NotNil)
	c.Assert(files, IsNil)

	// create relay log files
	relayDirsPath, relayFilesPath, safeTime := t.genRelayLogFiles(c, baseDir, 1, 1)

	// safeRelay older than safeTime
	safeRelay = &streamer.RelayLogInfo{
		SubDir:   t.uuids[1],
		Filename: t.relayFiles[1][0],
	}
	files, err = getRelayFilesBeforeFileAndTime(log.L(), baseDir, t.uuids, safeRelay, safeTime)
	c.Assert(err, IsNil)
	c.Assert(len(files), Equals, 1)
	c.Assert(files[0].dir, Equals, relayDirsPath[0])
	c.Assert(files[0].files, DeepEquals, relayFilesPath[0])
	c.Assert(files[0].hasAll, IsTrue)

	// safeRelay newer than safeTime
	safeRelay = &streamer.RelayLogInfo{
		SubDir:   t.uuids[2],
		Filename: t.relayFiles[2][0],
	}
	files, err = getRelayFilesBeforeFileAndTime(log.L(), baseDir, t.uuids, safeRelay, safeTime)
	c.Assert(err, IsNil)
	c.Assert(len(files), Equals, 2)
	c.Assert(files[0].dir, Equals, relayDirsPath[0])
	c.Assert(files[0].files, DeepEquals, relayFilesPath[0])
	c.Assert(files[0].hasAll, IsTrue)
	c.Assert(files[1].dir, Equals, relayDirsPath[1])
	c.Assert(files[1].files, DeepEquals, relayFilesPath[1][:2])
	c.Assert(files[1].hasAll, IsFalse)

	// write a fake meta file to first sub dir
	fakeMeta := filepath.Join(relayDirsPath[0], utils.MetaFilename)
	c.Assert(os.WriteFile(fakeMeta, []byte{}, 0o666), IsNil)

	// purge all relay log files in first and second sub dir, and some in third sub dir
	err = purgeRelayFilesBeforeFileAndTime(log.L(), baseDir, t.uuids, safeRelay, safeTime)
	c.Assert(err, IsNil)
	c.Assert(utils.IsDirExists(relayDirsPath[0]), IsFalse)
	c.Assert(utils.IsDirExists(relayDirsPath[1]), IsTrue)
	c.Assert(utils.IsDirExists(relayDirsPath[2]), IsTrue)
	c.Assert(utils.IsFileExists(relayFilesPath[1][0]), IsFalse)
	c.Assert(utils.IsFileExists(relayFilesPath[1][1]), IsFalse)
	c.Assert(utils.IsFileExists(relayFilesPath[1][2]), IsTrue)
}
