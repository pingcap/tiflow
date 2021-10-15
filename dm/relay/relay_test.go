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
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/binlog/event"
	"github.com/pingcap/dm/pkg/conn"
	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
	"github.com/pingcap/dm/relay/reader"
	"github.com/pingcap/dm/relay/retry"
	"github.com/pingcap/dm/relay/transformer"
	"github.com/pingcap/dm/relay/writer"
)

var _ = Suite(&testRelaySuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testRelaySuite struct{}

func (t *testRelaySuite) SetUpSuite(c *C) {
	c.Assert(log.InitLogger(&log.Config{}), IsNil)
}

func newRelayCfg(c *C, flavor string) *Config {
	dbCfg := getDBConfigForTest()
	return &Config{
		EnableGTID: false, // position mode, so auto-positioning can work
		Flavor:     flavor,
		RelayDir:   c.MkDir(),
		ServerID:   12321,
		From: config.DBConfig{
			Host:     dbCfg.Host,
			Port:     dbCfg.Port,
			User:     dbCfg.User,
			Password: dbCfg.Password,
		},
		ReaderRetry: retry.ReaderRetryConfig{
			BackoffRollback: 200 * time.Millisecond,
			BackoffMax:      1 * time.Second,
			BackoffMin:      1 * time.Millisecond,
			BackoffJitter:   true,
			BackoffFactor:   2,
		},
	}
}

func getDBConfigForTest() config.DBConfig {
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port, _ := strconv.Atoi(os.Getenv("MYSQL_PORT"))
	if port == 0 {
		port = 3306
	}
	user := os.Getenv("MYSQL_USER")
	if user == "" {
		user = "root"
	}
	password := os.Getenv("MYSQL_PSWD")
	return config.DBConfig{
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
	}
}

// mockReader is used only for relay testing.
type mockReader struct {
	result reader.Result
	err    error
}

func (r *mockReader) Start() error {
	return nil
}

func (r *mockReader) Close() error {
	return nil
}

func (r *mockReader) GetEvent(ctx context.Context) (reader.Result, error) {
	select {
	case <-ctx.Done():
		return reader.Result{}, ctx.Err()
	default:
	}
	return r.result, r.err
}

// mockWriter is used only for relay testing.
type mockWriter struct {
	result      writer.Result
	err         error
	latestEvent *replication.BinlogEvent
}

func (w *mockWriter) Start() error {
	return nil
}

func (w *mockWriter) Close() error {
	return nil
}

func (w *mockWriter) Recover(ctx context.Context) (writer.RecoverResult, error) {
	return writer.RecoverResult{}, nil
}

func (w *mockWriter) WriteEvent(ev *replication.BinlogEvent) (writer.Result, error) {
	w.latestEvent = ev // hold it
	return w.result, w.err
}

func (w *mockWriter) Flush() error {
	return nil
}

func (t *testRelaySuite) TestTryRecoverLatestFile(c *C) {
	var (
		uuid               = "24ecd093-8cec-11e9-aa0d-0242ac170002"
		uuidWithSuffix     = fmt.Sprintf("%s.000001", uuid)
		previousGTIDSetStr = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,406a3f61-690d-11e7-87c5-6c92bf46f384:123-456"
		latestGTIDStr1     = "3ccc475b-2343-11e7-be21-6c0b84d59f30:14"
		latestGTIDStr2     = "53bfca22-690d-11e7-8a62-18ded7a37b78:495"
		recoverGTIDSetStr  = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-17,53bfca22-690d-11e7-8a62-18ded7a37b78:1-505,406a3f61-690d-11e7-87c5-6c92bf46f384:1-456" // 406a3f61-690d-11e7-87c5-6c92bf46f384:123-456 --> 406a3f61-690d-11e7-87c5-6c92bf46f384:1-456
		greaterGITDSetStr  = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-20,53bfca22-690d-11e7-8a62-18ded7a37b78:1-510,406a3f61-690d-11e7-87c5-6c92bf46f384:123-456"
		filename           = "mysql-bin.000001"
		startPos           = gmysql.Position{Name: filename, Pos: 123}

		parser2  = parser.New()
		relayCfg = newRelayCfg(c, gmysql.MySQLFlavor)
		r        = NewRelay(relayCfg).(*Relay)
	)
	c.Assert(failpoint.Enable("github.com/pingcap/dm/pkg/utils/GetGTIDPurged", `return("406a3f61-690d-11e7-87c5-6c92bf46f384:1-122")`), IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/dm/pkg/utils/GetGTIDPurged")
	cfg := getDBConfigForTest()
	conn.InitMockDB(c)
	db, err := conn.DefaultDBProvider.Apply(cfg)
	c.Assert(err, IsNil)
	r.db = db
	c.Assert(r.Init(context.Background()), IsNil)
	// purge old relay dir
	f, err := os.Create(filepath.Join(r.cfg.RelayDir, "old_relay_log"))
	c.Assert(err, IsNil)
	f.Close()
	c.Assert(r.PurgeRelayDir(), IsNil)
	files, err := os.ReadDir(r.cfg.RelayDir)
	c.Assert(err, IsNil)
	c.Assert(files, HasLen, 0)

	c.Assert(r.meta.Load(), IsNil)

	// no file specified, no need to recover
	c.Assert(r.tryRecoverLatestFile(context.Background(), parser2), IsNil)

	// save position into meta
	c.Assert(r.meta.AddDir(uuid, &startPos, nil, 0), IsNil)

	// relay log file does not exists, no need to recover
	c.Assert(r.tryRecoverLatestFile(context.Background(), parser2), IsNil)

	// use a generator to generate some binlog events
	previousGTIDSet, err := gtid.ParserGTID(relayCfg.Flavor, previousGTIDSetStr)
	c.Assert(err, IsNil)
	latestGTID1, err := gtid.ParserGTID(relayCfg.Flavor, latestGTIDStr1)
	c.Assert(err, IsNil)
	latestGTID2, err := gtid.ParserGTID(relayCfg.Flavor, latestGTIDStr2)
	c.Assert(err, IsNil)
	g, events, data := genBinlogEventsWithGTIDs(c, relayCfg.Flavor, previousGTIDSet, latestGTID1, latestGTID2)

	// write events into relay log file
	err = os.WriteFile(filepath.Join(r.meta.Dir(), filename), data, 0o600)
	c.Assert(err, IsNil)

	// all events/transactions are complete, no need to recover
	c.Assert(r.tryRecoverLatestFile(context.Background(), parser2), IsNil)
	// now, we will update position/GTID set in meta to latest location in relay logs
	lastEvent := events[len(events)-1]
	pos := startPos
	pos.Pos = lastEvent.Header.LogPos
	t.verifyMetadata(c, r, uuidWithSuffix, pos, recoverGTIDSetStr, []string{uuidWithSuffix})

	// write some invalid data into the relay log file
	f, err = os.OpenFile(filepath.Join(r.meta.Dir(), filename), os.O_WRONLY|os.O_APPEND, 0o600)
	c.Assert(err, IsNil)
	_, err = f.Write([]byte("invalid event data"))
	c.Assert(err, IsNil)
	f.Close()

	// write a greater GTID sets in meta
	greaterGITDSet, err := gtid.ParserGTID(relayCfg.Flavor, greaterGITDSetStr)
	c.Assert(err, IsNil)
	c.Assert(r.SaveMeta(startPos, greaterGITDSet), IsNil)

	// invalid data truncated, meta updated
	c.Assert(r.tryRecoverLatestFile(context.Background(), parser2), IsNil)
	_, latestPos := r.meta.Pos()
	c.Assert(latestPos, DeepEquals, gmysql.Position{Name: filename, Pos: g.LatestPos})
	_, latestGTIDs := r.meta.GTID()
	recoverGTIDSet, err := gtid.ParserGTID(relayCfg.Flavor, recoverGTIDSetStr)
	c.Assert(err, IsNil)
	c.Assert(latestGTIDs.Equal(recoverGTIDSet), IsTrue) // verifyMetadata is not enough

	// no relay log file need to recover
	c.Assert(r.SaveMeta(minCheckpoint, latestGTIDs), IsNil)
	c.Assert(r.tryRecoverLatestFile(context.Background(), parser2), IsNil)
	_, latestPos = r.meta.Pos()
	c.Assert(latestPos, DeepEquals, minCheckpoint)
	_, latestGTIDs = r.meta.GTID()
	c.Assert(latestGTIDs.Contain(g.LatestGTID), IsTrue)
}

func (t *testRelaySuite) TestTryRecoverMeta(c *C) {
	var (
		uuid               = "24ecd093-8cec-11e9-aa0d-0242ac170002"
		previousGTIDSetStr = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,406a3f61-690d-11e7-87c5-6c92bf46f384:123-456"
		latestGTIDStr1     = "3ccc475b-2343-11e7-be21-6c0b84d59f30:14"
		latestGTIDStr2     = "53bfca22-690d-11e7-8a62-18ded7a37b78:495"
		// if no @@gtid_purged, 406a3f61-690d-11e7-87c5-6c92bf46f384:123-456 should be not changed
		recoverGTIDSetStr = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-17,53bfca22-690d-11e7-8a62-18ded7a37b78:1-505,406a3f61-690d-11e7-87c5-6c92bf46f384:123-456"
		filename          = "mysql-bin.000001"
		startPos          = gmysql.Position{Name: filename, Pos: 123}

		parser2  = parser.New()
		relayCfg = newRelayCfg(c, gmysql.MySQLFlavor)
		r        = NewRelay(relayCfg).(*Relay)
	)
	cfg := getDBConfigForTest()
	conn.InitMockDB(c)
	db, err := conn.DefaultDBProvider.Apply(cfg)
	c.Assert(err, IsNil)
	r.db = db
	c.Assert(r.Init(context.Background()), IsNil)
	recoverGTIDSet, err := gtid.ParserGTID(relayCfg.Flavor, recoverGTIDSetStr)
	c.Assert(err, IsNil)

	c.Assert(r.meta.AddDir(uuid, &startPos, nil, 0), IsNil)
	c.Assert(r.meta.Load(), IsNil)

	// use a generator to generate some binlog events
	previousGTIDSet, err := gtid.ParserGTID(relayCfg.Flavor, previousGTIDSetStr)
	c.Assert(err, IsNil)
	latestGTID1, err := gtid.ParserGTID(relayCfg.Flavor, latestGTIDStr1)
	c.Assert(err, IsNil)
	latestGTID2, err := gtid.ParserGTID(relayCfg.Flavor, latestGTIDStr2)
	c.Assert(err, IsNil)
	g, _, data := genBinlogEventsWithGTIDs(c, relayCfg.Flavor, previousGTIDSet, latestGTID1, latestGTID2)

	// write events into relay log file
	err = os.WriteFile(filepath.Join(r.meta.Dir(), filename), data, 0o600)
	c.Assert(err, IsNil)
	// write some invalid data into the relay log file to trigger a recover.
	f, err := os.OpenFile(filepath.Join(r.meta.Dir(), filename), os.O_WRONLY|os.O_APPEND, 0o600)
	c.Assert(err, IsNil)
	_, err = f.Write([]byte("invalid event data"))
	c.Assert(err, IsNil)
	f.Close()

	// recover with empty GTIDs.
	c.Assert(failpoint.Enable("github.com/pingcap/dm/pkg/utils/GetGTIDPurged", `return("")`), IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/dm/pkg/utils/GetGTIDPurged")
	c.Assert(r.tryRecoverLatestFile(context.Background(), parser2), IsNil)
	_, latestPos := r.meta.Pos()
	c.Assert(latestPos, DeepEquals, gmysql.Position{Name: filename, Pos: g.LatestPos})
	_, latestGTIDs := r.meta.GTID()
	c.Assert(latestGTIDs.Equal(recoverGTIDSet), IsTrue)

	// write some invalid data into the relay log file again.
	f, err = os.OpenFile(filepath.Join(r.meta.Dir(), filename), os.O_WRONLY|os.O_APPEND, 0o600)
	c.Assert(err, IsNil)
	_, err = f.Write([]byte("invalid event data"))
	c.Assert(err, IsNil)
	f.Close()

	// recover with the subset of GTIDs (previous GTID set).
	c.Assert(r.SaveMeta(startPos, previousGTIDSet), IsNil)
	c.Assert(r.tryRecoverLatestFile(context.Background(), parser2), IsNil)
	_, latestPos = r.meta.Pos()
	c.Assert(latestPos, DeepEquals, gmysql.Position{Name: filename, Pos: g.LatestPos})
	_, latestGTIDs = r.meta.GTID()
	c.Assert(latestGTIDs.Equal(recoverGTIDSet), IsTrue)
}

// genBinlogEventsWithGTIDs generates some binlog events used by testFileUtilSuite and testFileWriterSuite.
// now, its generated events including 3 DDL and 10 DML.
func genBinlogEventsWithGTIDs(c *C, flavor string, previousGTIDSet, latestGTID1, latestGTID2 gtid.Set) (*event.Generator, []*replication.BinlogEvent, []byte) {
	var (
		serverID  uint32 = 11
		latestPos uint32
		latestXID uint64 = 10

		allEvents = make([]*replication.BinlogEvent, 0, 50)
		allData   bytes.Buffer
	)

	// use a binlog event generator to generate some binlog events.
	g, err := event.NewGenerator(flavor, serverID, latestPos, latestGTID1, previousGTIDSet, latestXID)
	c.Assert(err, IsNil)

	// file header with FormatDescriptionEvent and PreviousGTIDsEvent
	events, data, err := g.GenFileHeader()
	c.Assert(err, IsNil)
	allEvents = append(allEvents, events...)
	allData.Write(data)

	// CREATE DATABASE/TABLE, 3 DDL
	queries := []string{
		"CREATE DATABASE `db`",
		"CREATE TABLE `db`.`tbl1` (c1 INT)",
		"CREATE TABLE `db`.`tbl2` (c1 INT)",
	}
	for _, query := range queries {
		events, data, err = g.GenDDLEvents("db", query)
		c.Assert(err, IsNil)
		allEvents = append(allEvents, events...)
		allData.Write(data)
	}

	// DMLs, 10 DML
	g.LatestGTID = latestGTID2 // use another latest GTID with different SID/DomainID
	var (
		tableID    uint64 = 8
		columnType        = []byte{gmysql.MYSQL_TYPE_LONG}
		eventType         = replication.WRITE_ROWS_EVENTv2
		schema            = "db"
		table             = "tbl1"
	)
	for i := 0; i < 10; i++ {
		insertRows := make([][]interface{}, 0, 1)
		insertRows = append(insertRows, []interface{}{int32(i)})
		dmlData := []*event.DMLData{
			{
				TableID:    tableID,
				Schema:     schema,
				Table:      table,
				ColumnType: columnType,
				Rows:       insertRows,
			},
		}
		events, data, err = g.GenDMLEvents(eventType, dmlData)
		c.Assert(err, IsNil)
		allEvents = append(allEvents, events...)
		allData.Write(data)
	}

	return g, allEvents, allData.Bytes()
}

func (t *testRelaySuite) TestHandleEvent(c *C) {
	// NOTE: we can test metrics later.
	var (
		reader2      = &mockReader{}
		transformer2 = transformer.NewTransformer(parser.New())
		writer2      = &mockWriter{}
		relayCfg     = newRelayCfg(c, gmysql.MariaDBFlavor)
		r            = NewRelay(relayCfg).(*Relay)

		eventHeader = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
		}
		binlogPos   = gmysql.Position{Name: "mysql-bin.666888", Pos: 4}
		rotateEv, _ = event.GenRotateEvent(eventHeader, 123, []byte(binlogPos.Name), uint64(binlogPos.Pos))
		queryEv, _  = event.GenQueryEvent(eventHeader, 123, 0, 0, 0, nil, nil, []byte("CREATE DATABASE db_relay_test"))
	)
	cfg := getDBConfigForTest()
	conn.InitMockDB(c)
	db, err := conn.DefaultDBProvider.Apply(cfg)
	c.Assert(err, IsNil)
	r.db = db
	c.Assert(r.Init(context.Background()), IsNil)
	// NOTE: we can mock meta later.
	c.Assert(r.meta.Load(), IsNil)
	c.Assert(r.meta.AddDir("24ecd093-8cec-11e9-aa0d-0242ac170002", nil, nil, 0), IsNil)

	// attach GTID sets to QueryEv
	queryEv2 := queryEv.Event.(*replication.QueryEvent)
	queryEv2.GSet, _ = gmysql.ParseGTIDSet(relayCfg.Flavor, "1-2-3")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	// reader return with an error
	for _, reader2.err = range []error{
		errors.New("reader error for testing"),
		replication.ErrChecksumMismatch,
		replication.ErrSyncClosed,
		replication.ErrNeedSyncAgain,
	} {
		_, handleErr := r.handleEvents(ctx, reader2, transformer2, writer2)
		c.Assert(errors.Cause(handleErr), Equals, reader2.err)
	}

	// reader return valid event
	reader2.err = nil
	reader2.result.Event = rotateEv

	// writer return error
	writer2.err = errors.New("writer error for testing")
	// return with the annotated writer error
	_, err = r.handleEvents(ctx, reader2, transformer2, writer2)
	c.Assert(errors.Cause(err), Equals, writer2.err)
	// after handle rotate event, we save and flush the meta immediately
	c.Assert(r.meta.Dirty(), Equals, false)

	// writer without error
	writer2.err = nil
	_, err = r.handleEvents(ctx, reader2, transformer2, writer2) // returned when ctx timeout
	c.Assert(errors.Cause(err), Equals, ctx.Err())
	// check written event
	c.Assert(writer2.latestEvent, Equals, reader2.result.Event)
	// check meta
	_, pos := r.meta.Pos()
	_, gs := r.meta.GTID()
	c.Assert(pos, DeepEquals, binlogPos)
	c.Assert(gs.String(), Equals, "") // no GTID sets in event yet

	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel2()

	// write a QueryEvent with GTID sets
	reader2.result.Event = queryEv
	_, err = r.handleEvents(ctx2, reader2, transformer2, writer2)
	c.Assert(errors.Cause(err), Equals, ctx.Err())
	// check written event
	c.Assert(writer2.latestEvent, Equals, reader2.result.Event)
	// check meta
	_, pos = r.meta.Pos()
	_, gs = r.meta.GTID()
	c.Assert(pos.Name, Equals, binlogPos.Name)
	c.Assert(pos.Pos, Equals, queryEv.Header.LogPos)
	c.Assert(gs.Origin(), DeepEquals, queryEv2.GSet) // got GTID sets

	// transformer return ignorable for the event
	reader2.err = nil
	reader2.result.Event = &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.HEARTBEAT_EVENT},
		Event:  &replication.GenericEvent{},
	}
	ctx4, cancel4 := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel4()
	_, err = r.handleEvents(ctx4, reader2, transformer2, writer2)
	c.Assert(errors.Cause(err), Equals, ctx.Err())
	select {
	case <-ctx4.Done():
	default:
		c.Fatalf("ignorable event for transformer not ignored")
	}

	// writer return ignorable for the event
	reader2.result.Event = queryEv
	writer2.result.Ignore = true
	ctx5, cancel5 := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel5()
	_, err = r.handleEvents(ctx5, reader2, transformer2, writer2)
	c.Assert(errors.Cause(err), Equals, ctx.Err())
	select {
	case <-ctx5.Done():
	default:
		c.Fatalf("ignorable event for writer not ignored")
	}
}

func (t *testRelaySuite) TestReSetupMeta(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), utils.DefaultDBTimeout)
	defer cancel()

	var (
		relayCfg = newRelayCfg(c, gmysql.MySQLFlavor)
		r        = NewRelay(relayCfg).(*Relay)
	)
	cfg := getDBConfigForTest()
	mockDB := conn.InitMockDB(c)
	db, err := conn.DefaultDBProvider.Apply(cfg)
	c.Assert(err, IsNil)
	r.db = db
	c.Assert(r.Init(context.Background()), IsNil)

	// empty metadata
	c.Assert(r.meta.Load(), IsNil)
	t.verifyMetadata(c, r, "", minCheckpoint, "", nil)

	// open connected DB and get its UUID
	defer func() {
		r.db.Close()
		r.db = nil
	}()
	mockGetServerUUID(mockDB)
	uuid, err := utils.GetServerUUID(ctx, r.db.DB, r.cfg.Flavor)
	c.Assert(err, IsNil)

	// re-setup meta with start pos adjusted
	r.cfg.EnableGTID = true
	r.cfg.BinlogGTID = "24ecd093-8cec-11e9-aa0d-0242ac170002:1-23"
	r.cfg.BinLogName = "mysql-bin.000005"

	c.Assert(r.setSyncConfig(), IsNil)
	// all adjusted gset should be empty since we didn't flush logs
	emptyGTID, err := gtid.ParserGTID(r.cfg.Flavor, "")
	c.Assert(err, IsNil)

	mockGetServerUUID(mockDB)
	mockGetRandomServerID(mockDB)
	//  mock AddGSetWithPurged
	mockDB.ExpectQuery("select @@GLOBAL.gtid_purged").WillReturnRows(sqlmock.NewRows([]string{"@@GLOBAL.gtid_purged"}).AddRow(""))
	c.Assert(failpoint.Enable("github.com/pingcap/dm/pkg/binlog/reader/MockGetEmptyPreviousGTIDFromGTIDSet", "return()"), IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/dm/pkg/binlog/reader/MockGetEmptyPreviousGTIDFromGTIDSet")
	c.Assert(r.reSetupMeta(ctx), IsNil)
	uuid001 := fmt.Sprintf("%s.000001", uuid)
	t.verifyMetadata(c, r, uuid001, gmysql.Position{Name: r.cfg.BinLogName, Pos: 4}, emptyGTID.String(), []string{uuid001})

	// re-setup meta again, often happen when connecting a server behind a VIP.
	mockGetServerUUID(mockDB)
	mockGetRandomServerID(mockDB)
	mockDB.ExpectQuery("select @@GLOBAL.gtid_purged").WillReturnRows(sqlmock.NewRows([]string{"@@GLOBAL.gtid_purged"}).AddRow(""))
	c.Assert(r.reSetupMeta(ctx), IsNil)
	uuid002 := fmt.Sprintf("%s.000002", uuid)
	t.verifyMetadata(c, r, uuid002, minCheckpoint, emptyGTID.String(), []string{uuid001, uuid002})

	r.cfg.BinLogName = "mysql-bin.000002"
	r.cfg.BinlogGTID = "24ecd093-8cec-11e9-aa0d-0242ac170002:1-50,24ecd093-8cec-11e9-aa0d-0242ac170003:1-50"
	r.cfg.UUIDSuffix = 2
	mockGetServerUUID(mockDB)
	mockGetRandomServerID(mockDB)
	mockDB.ExpectQuery("select @@GLOBAL.gtid_purged").WillReturnRows(sqlmock.NewRows([]string{"@@GLOBAL.gtid_purged"}).AddRow(""))
	c.Assert(r.reSetupMeta(ctx), IsNil)
	t.verifyMetadata(c, r, uuid002, gmysql.Position{Name: r.cfg.BinLogName, Pos: 4}, emptyGTID.String(), []string{uuid002})

	// re-setup meta again, often happen when connecting a server behind a VIP.
	mockGetServerUUID(mockDB)
	mockGetRandomServerID(mockDB)
	mockDB.ExpectQuery("select @@GLOBAL.gtid_purged").WillReturnRows(sqlmock.NewRows([]string{"@@GLOBAL.gtid_purged"}).AddRow(""))
	c.Assert(r.reSetupMeta(ctx), IsNil)
	uuid003 := fmt.Sprintf("%s.000003", uuid)
	t.verifyMetadata(c, r, uuid003, minCheckpoint, emptyGTID.String(), []string{uuid002, uuid003})
	c.Assert(mockDB.ExpectationsWereMet(), IsNil)
}

func (t *testRelaySuite) verifyMetadata(c *C, r *Relay, uuidExpected string,
	posExpected gmysql.Position, gsStrExpected string, uuidsExpected []string) {
	uuid, pos := r.meta.Pos()
	_, gs := r.meta.GTID()
	gsExpected, err := gtid.ParserGTID(gmysql.MySQLFlavor, gsStrExpected)
	c.Assert(err, IsNil)
	c.Assert(uuid, Equals, uuidExpected)
	c.Assert(pos, DeepEquals, posExpected)
	c.Assert(gs.Equal(gsExpected), IsTrue)

	indexFile := filepath.Join(r.cfg.RelayDir, utils.UUIDIndexFilename)
	UUIDs, err := utils.ParseUUIDIndex(indexFile)
	c.Assert(err, IsNil)
	c.Assert(UUIDs, DeepEquals, uuidsExpected)
}
