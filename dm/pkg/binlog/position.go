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

package binlog

import (
	"fmt"
	"strconv"
	"strings"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"go.uber.org/zap"
)

const (
	// in order to differ binlog position from multiple (switched) masters, we added a suffix which comes from relay log
	// subdirectory into binlogPos.Name. And we also need support position with RelaySubDirSuffix should always > position
	// without RelaySubDirSuffix, so we can continue from latter to former automatically.
	// convertedPos.BinlogName =
	//   originalPos.BinlogBaseName + posRelaySubDirSuffixSeparator + RelaySubDirSuffix + binlogFilenameSep + originalPos.BinlogSeq
	// eg. mysql-bin.000003 under folder c6ae5afe-c7a3-11e8-a19d-0242ac130006.000002 => mysql-bin|000002.000003
	// when new relay log subdirectory is created, RelaySubDirSuffix should increase.
	posRelaySubDirSuffixSeparator = utils.PosRelaySubDirSuffixSeparator
	// MinRelaySubDirSuffix is same as relay.MinRelaySubDirSuffix.
	MinRelaySubDirSuffix = 1
	// FileHeaderLen is the length of binlog file header.
	FileHeaderLen = 4
)

// MinPosition is the min binlog position.
var MinPosition = gmysql.Position{Pos: 4}

// PositionFromStr constructs a mysql.Position from a string representation like `mysql-bin.000001:2345`.
func PositionFromStr(s string) (gmysql.Position, error) {
	parsed := strings.Split(s, ":")
	if len(parsed) != 2 {
		return gmysql.Position{}, terror.ErrBinlogParsePosFromStr.Generatef("the format should be filename:pos, position string %s", s)
	}
	pos, err := strconv.ParseUint(parsed[1], 10, 32)
	if err != nil {
		return gmysql.Position{}, terror.ErrBinlogParsePosFromStr.Generatef("the pos should be digital, position string %s", s)
	}

	return gmysql.Position{
		Name: parsed[0],
		Pos:  uint32(pos),
	}, nil
}

func trimBrackets(s string) string {
	if len(s) > 2 && s[0] == '(' && s[len(s)-1] == ')' {
		return s[1 : len(s)-1]
	}
	return s
}

// PositionFromPosStr constructs a mysql.Position from a string representation like `(mysql-bin.000001, 2345)`.
func PositionFromPosStr(str string) (gmysql.Position, error) {
	s := trimBrackets(str)
	parsed := strings.Split(s, ", ")
	if len(parsed) != 2 {
		return gmysql.Position{}, terror.ErrBinlogParsePosFromStr.Generatef("invalid binlog pos, should be like (mysql-bin.000001, 2345), got %s", str)
	}
	pos, err := strconv.ParseUint(parsed[1], 10, 32)
	if err != nil {
		return gmysql.Position{}, terror.ErrBinlogParsePosFromStr.Generatef("the pos should be digital, position string %s", str)
	}

	return gmysql.Position{
		Name: parsed[0],
		Pos:  uint32(pos),
	}, nil
}

// RealMySQLPos parses a relay position and returns a mysql position and whether error occurs
// if parsed successfully and `RelaySubDirSuffix` in binlog filename exists, sets position Name to
// `originalPos.BinlogBaseName + binlogFilenameSep + originalPos.BinlogSeq`.
// if parsed failed returns the given position and the traced error.
func RealMySQLPos(pos gmysql.Position) (gmysql.Position, error) {
	parsed, err := utils.ParseFilename(pos.Name)
	if err != nil {
		return pos, err
	}

	sepIdx := strings.LastIndex(parsed.BaseName, posRelaySubDirSuffixSeparator)
	if sepIdx > 0 && sepIdx+len(posRelaySubDirSuffixSeparator) < len(parsed.BaseName) {
		if !verifyRelaySubDirSuffix(parsed.BaseName[sepIdx+len(posRelaySubDirSuffixSeparator):]) {
			// NOTE: still can't handle the case where `log-bin` has the format of `mysql-bin|666888`.
			return pos, nil // pos is just the real pos
		}
		return gmysql.Position{
			Name: utils.ConstructFilename(parsed.BaseName[:sepIdx], parsed.Seq),
			Pos:  pos.Pos,
		}, nil
	}

	return pos, nil
}

// ExtractSuffix extracts RelaySubDirSuffix from input name.
func ExtractSuffix(name string) (int, error) {
	if len(name) == 0 {
		return MinRelaySubDirSuffix, nil
	}
	filename, err := utils.ParseFilename(name)
	if err != nil {
		return 0, err
	}
	sepIdx := strings.LastIndex(filename.BaseName, posRelaySubDirSuffixSeparator)
	if sepIdx > 0 && sepIdx+len(posRelaySubDirSuffixSeparator) < len(filename.BaseName) {
		suffix := filename.BaseName[sepIdx+len(posRelaySubDirSuffixSeparator):]
		v, err := strconv.ParseInt(suffix, 10, 64)
		return int(v), err
	}
	return MinRelaySubDirSuffix, nil
}

// ExtractPos extracts (uuidWithSuffix, RelaySubDirSuffix, originalPos) from input position (originalPos or convertedPos).
// nolint:nakedret
func ExtractPos(pos gmysql.Position, uuids []string) (uuidWithSuffix string, relaySubDirSuffix string, realPos gmysql.Position, err error) {
	if len(uuids) == 0 {
		err = terror.ErrBinlogExtractPosition.New("empty UUIDs not valid")
		return
	}

	parsed, err := utils.ParseFilename(pos.Name)
	if err != nil {
		return
	}
	sepIdx := strings.LastIndex(parsed.BaseName, posRelaySubDirSuffixSeparator)
	if sepIdx > 0 && sepIdx+len(posRelaySubDirSuffixSeparator) < len(parsed.BaseName) {
		realBaseName, masterRelaySubDirSuffix := parsed.BaseName[:sepIdx], parsed.BaseName[sepIdx+len(posRelaySubDirSuffixSeparator):]
		if !verifyRelaySubDirSuffix(masterRelaySubDirSuffix) {
			err = terror.ErrBinlogExtractPosition.Generatef("invalid UUID suffix %s", masterRelaySubDirSuffix)
			return
		}

		// NOTE: still can't handle the case where `log-bin` has the format of `mysql-bin|666888` and UUID suffix `666888` exists.
		uuid := utils.GetUUIDBySuffix(uuids, masterRelaySubDirSuffix)

		if len(uuid) > 0 {
			// valid UUID found
			uuidWithSuffix = uuid
			relaySubDirSuffix = masterRelaySubDirSuffix
			realPos = gmysql.Position{
				Name: utils.ConstructFilename(realBaseName, parsed.Seq),
				Pos:  pos.Pos,
			}
		} else {
			err = terror.ErrBinlogExtractPosition.Generatef("UUID suffix %s with UUIDs %v not found", masterRelaySubDirSuffix, uuids)
		}
		return
	}

	// use the latest
	var suffixInt int
	uuid := uuids[len(uuids)-1]
	_, suffixInt, err = utils.ParseRelaySubDir(uuid)
	if err != nil {
		return
	}
	uuidWithSuffix = uuid
	relaySubDirSuffix = utils.SuffixIntToStr(suffixInt)
	realPos = pos // pos is realPos
	return
}

// verifyRelaySubDirSuffix verifies suffix whether is a valid relay log subdirectory suffix.
func verifyRelaySubDirSuffix(suffix string) bool {
	v, err := strconv.ParseInt(suffix, 10, 64)
	if err != nil || v <= 0 {
		return false
	}
	return true
}

// RemoveRelaySubDirSuffix removes relay dir suffix from binlog filename of a position.
// for example: mysql-bin|000001.000002 -> mysql-bin.000002.
func RemoveRelaySubDirSuffix(pos gmysql.Position) gmysql.Position {
	realPos, err := RealMySQLPos(pos)
	if err != nil {
		// just return the origin pos
		return pos
	}

	return realPos
}

// VerifyBinlogPos verify binlog pos string.
func VerifyBinlogPos(pos string) (*gmysql.Position, error) {
	binlogPosStr := utils.TrimQuoteMark(pos)
	pos2, err := PositionFromStr(binlogPosStr)
	if err != nil {
		return nil, terror.ErrVerifyHandleErrorArgs.Generatef("invalid --binlog-pos %s in handle-error operation: %s", binlogPosStr, terror.Message(err))
	}
	return &pos2, nil
}

// ComparePosition returns:
//
//	1 if pos1 is bigger than pos2
//	0 if pos1 is equal to pos2
//	-1 if pos1 is less than pos2
func ComparePosition(pos1, pos2 gmysql.Position) int {
	adjustedPos1 := RemoveRelaySubDirSuffix(pos1)
	adjustedPos2 := RemoveRelaySubDirSuffix(pos2)

	// means both pos1 and pos2 have uuid in name, so need also compare the uuid
	if adjustedPos1.Name != pos1.Name && adjustedPos2.Name != pos2.Name {
		return pos1.Compare(pos2)
	}

	return adjustedPos1.Compare(adjustedPos2)
}

// Location identifies the location of binlog events.
type Location struct {
	// a structure represents the file offset in binlog file
	Position gmysql.Position
	// executed GTID set at this location.
	gtidSet gmysql.GTIDSet
	// used to distinguish injected events by DM when it's not 0
	Suffix int
}

// ZeroLocation returns a new Location. The flavor should not be empty.
func ZeroLocation(flavor string) (Location, error) {
	gset, err := gtid.ZeroGTIDSet(flavor)
	if err != nil {
		return Location{}, err
	}
	return Location{
		Position: MinPosition,
		gtidSet:  gset,
	}, nil
}

// MustZeroLocation returns a new Location. The flavor must not be empty.
// in DM the flavor is adjusted before write to etcd.
func MustZeroLocation(flavor string) Location {
	return Location{
		Position: MinPosition,
		gtidSet:  gtid.MustZeroGTIDSet(flavor),
	}
}

// NewLocation creates a new Location from given binlog position and GTID.
func NewLocation(pos gmysql.Position, gset gmysql.GTIDSet) Location {
	return Location{
		Position: pos,
		gtidSet:  gset,
	}
}

func (l Location) String() string {
	if l.Suffix == 0 {
		return fmt.Sprintf("position: %v, gtid-set: %s", l.Position, l.GTIDSetStr())
	}
	return fmt.Sprintf("position: %v, gtid-set: %s, suffix: %d", l.Position, l.GTIDSetStr(), l.Suffix)
}

// GTIDSetStr returns gtid set's string.
func (l Location) GTIDSetStr() string {
	gsetStr := ""
	if l.gtidSet != nil {
		gsetStr = l.gtidSet.String()
	}

	return gsetStr
}

// Clone clones a same Location.
func (l Location) Clone() Location {
	return l.CloneWithFlavor("")
}

// CloneWithFlavor clones the location, and if the GTIDSet is nil, will create a GTIDSet with specified flavor.
func (l Location) CloneWithFlavor(flavor string) Location {
	var newGTIDSet gmysql.GTIDSet
	if l.gtidSet != nil {
		newGTIDSet = l.gtidSet.Clone()
	} else if len(flavor) != 0 {
		newGTIDSet = gtid.MustZeroGTIDSet(flavor)
	}

	return Location{
		Position: l.Position,
		gtidSet:  newGTIDSet,
		Suffix:   l.Suffix,
	}
}

// CompareLocation returns:
//
//	1 if point1 is bigger than point2
//	0 if point1 is equal to point2
//	-1 if point1 is less than point2
func CompareLocation(location1, location2 Location, cmpGTID bool) int {
	if cmpGTID {
		cmp, canCmp := CompareGTID(location1.gtidSet, location2.gtidSet)
		if canCmp {
			if cmp != 0 {
				return cmp
			}
			return compareInjectSuffix(location1.Suffix, location2.Suffix)
		}

		// if can't compare by GTIDSet, then compare by position
		log.L().Warn("gtidSet can't be compared, will compare by position", zap.Stringer("location1", location1), zap.Stringer("location2", location2))
	}

	cmp := ComparePosition(location1.Position, location2.Position)
	if cmp != 0 {
		return cmp
	}
	return compareInjectSuffix(location1.Suffix, location2.Suffix)
}

// IsFreshPosition returns true when location1 is a fresh location without any info.
func IsFreshPosition(location Location, flavor string, cmpGTID bool) bool {
	zeroLocation := MustZeroLocation(flavor)
	if cmpGTID {
		cmp, canCmp := CompareGTID(location.gtidSet, zeroLocation.gtidSet)
		if canCmp {
			switch {
			case cmp > 0:
				return false
			case cmp < 0:
				// should not happen
				return true
			}
			// empty GTIDSet, then compare by position
			log.L().Warn("given gtidSets is empty, will compare by position", zap.Stringer("location", location))
		} else {
			// if can't compare by GTIDSet, then compare by position
			log.L().Warn("gtidSet can't be compared, will compare by position", zap.Stringer("location", location))
		}
	}

	cmp := ComparePosition(location.Position, zeroLocation.Position)
	if cmp != 0 {
		return cmp <= 0
	}
	return compareInjectSuffix(location.Suffix, zeroLocation.Suffix) <= 0
}

// CompareGTID returns:
//
//	1, true if gSet1 is bigger than gSet2
//	0, true if gSet1 is equal to gSet2
//	-1, true if gSet1 is less than gSet2
//
// but if can't compare gSet1 and gSet2, will returns 0, false.
var (
	emptyMySQLGTIDSet, _   = gmysql.ParseMysqlGTIDSet("")
	emptyMariaDBGTIDSet, _ = gmysql.ParseMariadbGTIDSet("")
)

func CheckGTIDSetEmpty(gSet gmysql.GTIDSet) bool {
	return gSet == nil || gSet.Equal(emptyMySQLGTIDSet) || gSet.Equal(emptyMariaDBGTIDSet)
}

func CompareGTID(gSet1, gSet2 gmysql.GTIDSet) (int, bool) {
	gSetIsEmpty1 := gtid.IsZeroGTIDSet(gSet1)
	gSetIsEmpty2 := gtid.IsZeroGTIDSet(gSet2)

	switch {
	case gSetIsEmpty1 && gSetIsEmpty2:
		// both gSet1 and gSet2 is nil
		return 0, true
	case gSetIsEmpty1:
		return -1, true
	case gSetIsEmpty2:
		return 1, true
	}

	// both gSet1 and gSet2 is not nil
	contain1 := gSet1.Contain(gSet2)
	contain2 := gSet2.Contain(gSet1)
	if contain1 && contain2 {
		// gtidSet1 contains gtidSet2 and gtidSet2 contains gtidSet1 means gtidSet1 equals to gtidSet2,
		return 0, true
	}

	if contain1 {
		return 1, true
	} else if contain2 {
		return -1, true
	}

	return 0, false
}

func compareInjectSuffix(lhs, rhs int) int {
	switch {
	case lhs < rhs:
		return -1
	case lhs > rhs:
		return 1
	default:
		return 0
	}
}

// ResetSuffix set suffix to 0.
func (l *Location) ResetSuffix() {
	l.Suffix = 0
}

// CopyWithoutSuffixFrom copies a same Location without suffix. Note that gtidSet is shared.
func (l *Location) CopyWithoutSuffixFrom(from Location) {
	l.Position = from.Position
	l.gtidSet = from.gtidSet
}

// SetGTID set new gtid for location.
// TODO: don't change old Location and return a new one to copy-on-write.
func (l *Location) SetGTID(gset gmysql.GTIDSet) error {
	l.gtidSet = gset
	return nil
}

// GetGTID return gtidSet of Location.
// NOTE: for most cases you should clone before call Update on the returned GTID
// set, unless you know there's no other reference using the GTID set.
func (l *Location) GetGTID() gmysql.GTIDSet {
	return l.gtidSet
}

// Update will update GTIDSet of Location.
// caller should be aware that this will change the GTID set of other copies.
// TODO: don't change old Location and return a new one to copy-on-write.
func (l *Location) Update(gtidStr string) error {
	return l.gtidSet.Update(gtidStr)
}
