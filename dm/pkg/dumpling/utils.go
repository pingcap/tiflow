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

package dumpling

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/docker/go-units"
	"github.com/go-mysql-org/go-mysql/mysql"
	brstorage "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/dumpling/export"
	"github.com/spf13/pflag"

	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/storage"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

// DefaultTableFilter is the default table filter for dumpling.
var DefaultTableFilter = []string{"*.*", export.DefaultTableFilter}

// ParseMetaData parses mydumper's output meta file and returns binlog location.
// since v2.0.0, dumpling maybe configured to output master status after connection pool is established,
// we return this location as well.
// If `extStorage` is nil, we will use `dir` to open a storage.
func ParseMetaData(
	ctx context.Context,
	dir string,
	filename string,
	flavor string,
	extStorage brstorage.ExternalStorage,
) (*binlog.Location, *binlog.Location, error) {
	fd, err := storage.OpenFile(ctx, dir, filename, extStorage)
	if err != nil {
		return nil, nil, err
	}
	defer fd.Close()

	return parseMetaDataByReader(filename, flavor, fd)
}

// ParseMetaData parses mydumper's output meta file by created reader and returns binlog location.
func parseMetaDataByReader(filename, flavor string, rd io.Reader) (*binlog.Location, *binlog.Location, error) {
	invalidErr := fmt.Errorf("file %s invalid format", filename)

	var (
		pos          mysql.Position
		gtidStr      string
		useLocation2 = false
		pos2         mysql.Position
		gtidStr2     string

		locPtr  *binlog.Location
		locPtr2 *binlog.Location
	)

	br := bufio.NewReader(rd)

	parsePosAndGTID := func(pos *mysql.Position, gtid *string) error {
		for {
			line, err2 := br.ReadString('\n')
			if err2 != nil {
				return err2
			}
			line = strings.TrimSpace(line)
			if len(line) == 0 {
				return nil
			}
			parts := strings.SplitN(line, ":", 2)
			if len(parts) != 2 {
				continue
			}
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			switch key {
			case "Log":
				pos.Name = value
			case "Pos":
				pos64, err3 := strconv.ParseUint(value, 10, 32)
				if err3 != nil {
					return err3
				}
				pos.Pos = uint32(pos64)
			case "GTID":
				// multiple GTID sets may cross multiple lines, continue to read them.
				following, err3 := readFollowingGTIDs(br, flavor)
				if err3 != nil {
					return err3
				}
				*gtid = value + following
				return nil
			}
		}
	}

	for {
		line, err2 := br.ReadString('\n')
		if err2 == io.EOF {
			break
		} else if err2 != nil {
			return nil, nil, err2
		}
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		switch line {
		case "SHOW MASTER STATUS:":
			if err3 := parsePosAndGTID(&pos, &gtidStr); err3 != nil {
				return nil, nil, err3
			}
		case "SHOW SLAVE STATUS:":
			// ref: https://github.com/maxbube/mydumper/blob/master/mydumper.c#L434
			for {
				line, err3 := br.ReadString('\n')
				if err3 != nil {
					return nil, nil, err3
				}
				line = strings.TrimSpace(line)
				if len(line) == 0 {
					break
				}
			}
		case "SHOW MASTER STATUS: /* AFTER CONNECTION POOL ESTABLISHED */":
			useLocation2 = true
			if err3 := parsePosAndGTID(&pos2, &gtidStr2); err3 != nil {
				return nil, nil, err3
			}
		default:
			// do nothing for Started dump, Finished dump...
		}
	}

	if len(pos.Name) == 0 || pos.Pos == uint32(0) {
		return nil, nil, terror.ErrMetadataNoBinlogLoc.Generate(filename)
	}

	gset, err := gtid.ParserGTID(flavor, gtidStr)
	if err != nil {
		return nil, nil, invalidErr
	}
	loc := binlog.InitLocation(pos, gset)
	locPtr = &loc

	if useLocation2 {
		if len(pos2.Name) == 0 || pos2.Pos == uint32(0) {
			return nil, nil, invalidErr
		}
		gset2, err := gtid.ParserGTID(flavor, gtidStr2)
		if err != nil {
			return nil, nil, invalidErr
		}
		loc2 := binlog.InitLocation(pos2, gset2)
		locPtr2 = &loc2
	}

	return locPtr, locPtr2, nil
}

func readFollowingGTIDs(br *bufio.Reader, flavor string) (string, error) {
	var following strings.Builder
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			return following.String(), nil // return the previous, not including the last line.
		} else if err != nil {
			return "", err
		}

		line = strings.TrimSpace(line)
		if len(line) == 0 {
			return following.String(), nil // end with empty line.
		}

		end := len(line)
		if strings.HasSuffix(line, ",") {
			end = len(line) - 1
		}

		// try parse to verify it
		_, err = gtid.ParserGTID(flavor, line[:end])
		if err != nil {
			// nolint:nilerr
			return following.String(), nil // return the previous, not including this non-GTID line.
		}

		following.WriteString(line)
	}
}

// ParseFileSize parses the size in MiB from input.
func ParseFileSize(fileSizeStr string, defaultSize uint64) (uint64, error) {
	var fileSize uint64
	if len(fileSizeStr) == 0 {
		fileSize = defaultSize
	} else if fileSizeMB, err := strconv.ParseUint(fileSizeStr, 10, 64); err == nil {
		fileSize = fileSizeMB * units.MiB
	} else if size, err := units.RAMInBytes(fileSizeStr); err == nil {
		fileSize = uint64(size)
	} else {
		return 0, err
	}
	return fileSize, nil
}

// ParseArgLikeBash parses list arguments like bash, which helps us to run
// executable command via os/exec more likely running from bash.
func ParseArgLikeBash(args []string) []string {
	result := make([]string, 0, len(args))
	for _, arg := range args {
		parsedArg := trimOutQuotes(arg)
		result = append(result, parsedArg)
	}
	return result
}

// trimOutQuotes trims a pair of single quotes or a pair of double quotes from arg.
func trimOutQuotes(arg string) string {
	argLen := len(arg)
	if argLen >= 2 {
		if arg[0] == '"' && arg[argLen-1] == '"' {
			return arg[1 : argLen-1]
		}
		if arg[0] == '\'' && arg[argLen-1] == '\'' {
			return arg[1 : argLen-1]
		}
	}
	return arg
}

func ParseExtraArgs(logger *log.Logger, dumpCfg *export.Config, args []string) error {
	var (
		dumplingFlagSet = pflag.NewFlagSet("dumpling", pflag.ContinueOnError)
		fileSizeStr     string
		tablesList      []string
		filters         []string
		noLocks         bool
	)

	dumplingFlagSet.StringSliceVarP(&dumpCfg.Databases, "database", "B", dumpCfg.Databases, "Database to dump")
	dumplingFlagSet.StringSliceVarP(&tablesList, "tables-list", "T", nil, "Comma delimited table list to dump; must be qualified table names")
	dumplingFlagSet.IntVarP(&dumpCfg.Threads, "threads", "t", dumpCfg.Threads, "Number of goroutines to use, default 4")
	dumplingFlagSet.StringVarP(&fileSizeStr, "filesize", "F", "", "The approximate size of output file")
	dumplingFlagSet.Uint64VarP(&dumpCfg.StatementSize, "statement-size", "s", dumpCfg.StatementSize, "Attempted size of INSERT statement in bytes")
	dumplingFlagSet.StringVar(&dumpCfg.Consistency, "consistency", dumpCfg.Consistency, "Consistency level during dumping: {auto|none|flush|lock|snapshot}")
	dumplingFlagSet.StringVar(&dumpCfg.Snapshot, "snapshot", dumpCfg.Snapshot, "Snapshot position. Valid only when consistency=snapshot")
	dumplingFlagSet.BoolVarP(&dumpCfg.NoViews, "no-views", "W", dumpCfg.NoViews, "Do not dump views")
	dumplingFlagSet.Uint64VarP(&dumpCfg.Rows, "rows", "r", dumpCfg.Rows, "Split table into chunks of this many rows, default unlimited")
	dumplingFlagSet.StringVar(&dumpCfg.Where, "where", dumpCfg.Where, "Dump only selected records")
	dumplingFlagSet.BoolVar(&dumpCfg.EscapeBackslash, "escape-backslash", dumpCfg.EscapeBackslash, "Use backslash to escape quotation marks")
	dumplingFlagSet.StringArrayVarP(&filters, "filter", "f", DefaultTableFilter, "Filter to select which tables to dump")
	dumplingFlagSet.StringVar(&dumpCfg.Security.CAPath, "ca", dumpCfg.Security.CAPath, "The path name to the certificate authority file for TLS connection")
	dumplingFlagSet.StringVar(&dumpCfg.Security.CertPath, "cert", dumpCfg.Security.CertPath, "The path name to the client certificate file for TLS connection")
	dumplingFlagSet.StringVar(&dumpCfg.Security.KeyPath, "key", dumpCfg.Security.KeyPath, "The path name to the client private key file for TLS connection")
	dumplingFlagSet.BoolVar(&noLocks, "no-locks", false, "")
	dumplingFlagSet.BoolVar(&dumpCfg.TransactionalConsistency, "transactional-consistency", true, "Only support transactional consistency")

	err := dumplingFlagSet.Parse(args)
	if err != nil {
		return err
	}

	// compatibility for `--no-locks`
	if noLocks {
		logger.Warn("`--no-locks` is replaced by `--consistency none` since v2.0.0")
		// it's default consistency or by meaning of "auto", we could overwrite it by `none`
		if dumpCfg.Consistency == "auto" {
			dumpCfg.Consistency = "none"
		} else if dumpCfg.Consistency != "none" {
			return errors.New("cannot both specify `--no-locks` and `--consistency` other than `none`")
		}
	}

	if fileSizeStr != "" {
		dumpCfg.FileSize, err = ParseFileSize(fileSizeStr, export.UnspecifiedSize)
		if err != nil {
			return err
		}
	}

	if len(tablesList) > 0 || !utils.NonRepeatStringsEqual(DefaultTableFilter, filters) {
		ff, err2 := export.ParseTableFilter(tablesList, filters)
		if err2 != nil {
			return err2
		}
		dumpCfg.TableFilter = ff // overwrite `block-allow-list`.
		logger.Warn("overwrite `block-allow-list` by `tables-list` or `filter`")
	}

	return nil
}
