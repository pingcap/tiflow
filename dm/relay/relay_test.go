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

	"github.com/BurntSushi/toml"
	"github.com/DATA-DOG/go-sqlmock"
	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tiflow/dm/config/dbconfig"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/stretchr/testify/require"
)

func newRelayCfg(t *testing.T, flavor string) *Config {
	dbCfg := getDBConfigForTest()
	return &Config{
		EnableGTID: false, // position mode, so auto-positioning can work
		Flavor:     flavor,
		RelayDir:   t.TempDir(),
		ServerID:   12321,
		From: dbconfig.DBConfig{
			Host:     dbCfg.Host,
			Port:     dbCfg.Port,
			User:     dbCfg.User,
			Password: dbCfg.Password,
		},
		ReaderRetry: ReaderRetryConfig{
			BackoffRollback: 200 * time.Millisecond,
			BackoffMax:      1 * time.Second,
			BackoffMin:      1 * time.Millisecond,
			BackoffJitter:   true,
			BackoffFactor:   2,
		},
	}
}

func getDBConfigForTest() *dbconfig.DBConfig {
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
	return &dbconfig.DBConfig{
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
	}
}

// mockReader is used only for relay testing.
type mockReader struct {
	result RResult
	err    error
}

func (r *mockReader) Start() error {
	return nil
}

func (r *mockReader) Close() error {
	return nil
}

func (r *mockReader) GetEvent(ctx context.Context) (RResult, error) {
	select {
	case <-ctx.Done():
		return RResult{}, ctx.Err()
	default:
	}
	return r.result, r.err
}

// mockWriter is used only for relay testing.
type mockWriter struct {
	result      WResult
	err         error
	latestEvent *replication.BinlogEvent
}

func (w *mockWriter) IsActive(uuid, filename string) (bool, int64) {
	return false, 0
}

func (w *mockWriter) Close() error {
	return nil
}

func (w *mockWriter) Init(relayDir, filename string) {
}

func (w *mockWriter) WriteEvent(ev *replication.BinlogEvent) (WResult, error) {
	w.latestEvent = ev // hold it
	return w.result, w.err
}

func (w *mockWriter) Flush() error {
	return nil
}

func TestTryRecoverLatestFile(t *testing.T) {
	var (
		uuid               = "24ecd093-8cec-11e9-aa0d-0242ac170002"
		uuidWithSuffix     = fmt.Sprintf("%s.000001", uuid)
		previousGTIDSetStr = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,406a3f61-690d-11e7-87c5-6c92bf46f384:123-456"
		latestGTIDStr1     = "3ccc475b-2343-11e7-be21-6c0b84d59f30:14"
		latestGTIDStr2     = "53bfca22-690d-11e7-8a62-18ded7a37b78:495"
		recoverGTIDSetStr  = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-18,53bfca22-690d-11e7-8a62-18ded7a37b78:1-505,406a3f61-690d-11e7-87c5-6c92bf46f384:1-456" // 406a3f61-690d-11e7-87c5-6c92bf46f384:123-456 --> 406a3f61-690d-11e7-87c5-6c92bf46f384:1-456
		greaterGITDSetStr  = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-20,53bfca22-690d-11e7-8a62-18ded7a37b78:1-510,406a3f61-690d-11e7-87c5-6c92bf46f384:123-456"
		filename           = "mysql-bin.000001"
		startPos           = gmysql.Position{Name: filename, Pos: 123}

		parser2  = parser.New()
		relayCfg = newRelayCfg(t, gmysql.MySQLFlavor)
		r        = NewRelay(relayCfg).(*Relay)
	)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tiflow/dm/pkg/conn/GetGTIDPurged", `return("406a3f61-690d-11e7-87c5-6c92bf46f384:1-122")`))
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/pkg/conn/GetGTIDPurged")
	cfg := getDBConfigForTest()
	conn.InitMockDB(t)
	db, err := conn.GetUpstreamDB(cfg)
	require.NoError(t, err)
	r.db = db
	require.NoError(t, r.Init(context.Background()))
	// purge old relay dir
	f, err := os.Create(filepath.Join(r.cfg.RelayDir, "old_relay_log"))
	require.NoError(t, err)
	f.Close()
	require.NoError(t, r.PurgeRelayDir())
	files, err := os.ReadDir(r.cfg.RelayDir)
	require.NoError(t, err)
	require.Len(t, files, 0)

	require.NoError(t, r.meta.Load())

	// no file specified, no need to recover
	require.Nil(t, r.tryRecoverLatestFile(context.Background(), parser2))

	// save position into meta
	require.Nil(t, r.meta.AddDir(uuid, &startPos, nil, 0))

	// relay log file does not exists, no need to recover
	require.Nil(t, r.tryRecoverLatestFile(context.Background(), parser2))

	// use a generator to generate some binlog events
	previousGTIDSet, err := gtid.ParserGTID(relayCfg.Flavor, previousGTIDSetStr)
	require.NoError(t, err)
	latestGTID1, err := gtid.ParserGTID(relayCfg.Flavor, latestGTIDStr1)
	require.NoError(t, err)
	latestGTID2, err := gtid.ParserGTID(relayCfg.Flavor, latestGTIDStr2)
	require.NoError(t, err)
	g, events, data := genBinlogEventsWithGTIDs(t, relayCfg.Flavor, previousGTIDSet, latestGTID1, latestGTID2)

	// write events into relay log file
	err = os.WriteFile(filepath.Join(r.meta.Dir(), filename), data, 0o600)
	require.NoError(t, err)

	// all events/transactions are complete, no need to recover
	require.Nil(t, r.tryRecoverLatestFile(context.Background(), parser2))
	// now, we will update position/GTID set in meta to latest location in relay logs
	lastEvent := events[len(events)-1]
	pos := startPos
	pos.Pos = lastEvent.Header.LogPos
	verifyMetadata(t, r, uuidWithSuffix, pos, recoverGTIDSetStr, []string{uuidWithSuffix})

	// write some invalid data into the relay log file
	f, err = os.OpenFile(filepath.Join(r.meta.Dir(), filename), os.O_WRONLY|os.O_APPEND, 0o600)
	require.NoError(t, err)
	_, err = f.Write([]byte("invalid event data"))
	require.NoError(t, err)
	f.Close()

	// write a greater GTID sets in meta
	greaterGITDSet, err := gtid.ParserGTID(relayCfg.Flavor, greaterGITDSetStr)
	require.NoError(t, err)
	require.Nil(t, r.SaveMeta(startPos, greaterGITDSet))

	// invalid data truncated, meta updated
	require.Nil(t, r.tryRecoverLatestFile(context.Background(), parser2))
	_, latestPos := r.meta.Pos()
	require.Equal(t, gmysql.Position{Name: filename, Pos: g.LatestPos}, latestPos)
	_, latestGTIDs := r.meta.GTID()
	recoverGTIDSet, err := gtid.ParserGTID(relayCfg.Flavor, recoverGTIDSetStr)
	require.NoError(t, err)
	require.True(t, latestGTIDs.Equal(recoverGTIDSet)) // verifyMetadata is not enough

	// no relay log file need to recover
	require.Nil(t, r.SaveMeta(minCheckpoint, latestGTIDs))
	require.Nil(t, r.tryRecoverLatestFile(context.Background(), parser2))
	_, latestPos = r.meta.Pos()
	require.Equal(t, minCheckpoint, latestPos)
	_, latestGTIDs = r.meta.GTID()
	require.True(t, latestGTIDs.Contain(g.LatestGTID))
}

func TestTryRecoverMeta(t *testing.T) {
	var (
		uuid               = "24ecd093-8cec-11e9-aa0d-0242ac170002"
		previousGTIDSetStr = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,406a3f61-690d-11e7-87c5-6c92bf46f384:123-456"
		latestGTIDStr1     = "3ccc475b-2343-11e7-be21-6c0b84d59f30:14"
		latestGTIDStr2     = "53bfca22-690d-11e7-8a62-18ded7a37b78:495"
		// if no @@gtid_purged, 406a3f61-690d-11e7-87c5-6c92bf46f384:123-456 should be not changed
		recoverGTIDSetStr = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-18,53bfca22-690d-11e7-8a62-18ded7a37b78:1-505,406a3f61-690d-11e7-87c5-6c92bf46f384:123-456"
		filename          = "mysql-bin.000001"
		startPos          = gmysql.Position{Name: filename, Pos: 123}

		parser2  = parser.New()
		relayCfg = newRelayCfg(t, gmysql.MySQLFlavor)
		r        = NewRelay(relayCfg).(*Relay)
	)
	cfg := getDBConfigForTest()
	conn.InitMockDB(t)
	db, err := conn.GetUpstreamDB(cfg)
	require.NoError(t, err)
	r.db = db
	require.NoError(t, r.Init(context.Background()))
	recoverGTIDSet, err := gtid.ParserGTID(relayCfg.Flavor, recoverGTIDSetStr)
	require.NoError(t, err)

	require.Nil(t, r.meta.AddDir(uuid, &startPos, nil, 0))
	require.NoError(t, r.meta.Load())

	// use a generator to generate some binlog events
	previousGTIDSet, err := gtid.ParserGTID(relayCfg.Flavor, previousGTIDSetStr)
	require.NoError(t, err)
	latestGTID1, err := gtid.ParserGTID(relayCfg.Flavor, latestGTIDStr1)
	require.NoError(t, err)
	latestGTID2, err := gtid.ParserGTID(relayCfg.Flavor, latestGTIDStr2)
	require.NoError(t, err)
	g, _, data := genBinlogEventsWithGTIDs(t, relayCfg.Flavor, previousGTIDSet, latestGTID1, latestGTID2)

	// write events into relay log file
	err = os.WriteFile(filepath.Join(r.meta.Dir(), filename), data, 0o600)
	require.NoError(t, err)
	// write some invalid data into the relay log file to trigger a recover.
	f, err := os.OpenFile(filepath.Join(r.meta.Dir(), filename), os.O_WRONLY|os.O_APPEND, 0o600)
	require.NoError(t, err)
	_, err = f.Write([]byte("invalid event data"))
	require.NoError(t, err)
	f.Close()

	// recover with empty GTIDs.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tiflow/dm/pkg/conn/GetGTIDPurged", `return("")`))
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/pkg/conn/GetGTIDPurged")
	require.Nil(t, r.tryRecoverLatestFile(context.Background(), parser2))
	_, latestPos := r.meta.Pos()
	require.Equal(t, gmysql.Position{Name: filename, Pos: g.LatestPos}, latestPos)
	_, latestGTIDs := r.meta.GTID()
	require.True(t, latestGTIDs.Equal(recoverGTIDSet))

	// write some invalid data into the relay log file again.
	f, err = os.OpenFile(filepath.Join(r.meta.Dir(), filename), os.O_WRONLY|os.O_APPEND, 0o600)
	require.NoError(t, err)
	_, err = f.Write([]byte("invalid event data"))
	require.NoError(t, err)
	f.Close()

	// recover with the subset of GTIDs (previous GTID set).
	require.Nil(t, r.SaveMeta(startPos, previousGTIDSet))
	require.Nil(t, r.tryRecoverLatestFile(context.Background(), parser2))
	_, latestPos = r.meta.Pos()
	require.Equal(t, gmysql.Position{Name: filename, Pos: g.LatestPos}, latestPos)
	_, latestGTIDs = r.meta.GTID()
	require.True(t, latestGTIDs.Equal(recoverGTIDSet))
}

type dummyListener bool

func (d *dummyListener) OnEvent(e *replication.BinlogEvent) {
	*d = true
}

func TestListener(t *testing.T) {
	relay := NewRelay(&Config{}).(*Relay)
	require.Equal(t, 0, len(relay.listeners))

	lis := dummyListener(false)
	relay.RegisterListener(&lis)
	require.Equal(t, 1, len(relay.listeners))

	relay.notify(nil)
	require.Equal(t, true, bool(lis))

	relay.UnRegisterListener(&lis)
	require.Equal(t, 0, len(relay.listeners))
	lis = false
	relay.notify(nil)
	require.Equal(t, false, bool(lis))
}

// genBinlogEventsWithGTIDs generates some binlog events used by testFileUtilSuite and testFileWriterSuite.
// now, its generated events including 3 DDL and 10 DML.
func genBinlogEventsWithGTIDs(t *testing.T, flavor string, previousGTIDSet, latestGTID1, latestGTID2 gmysql.GTIDSet) (*event.Generator, []*replication.BinlogEvent, []byte) {
	var (
		serverID  uint32 = 11
		latestPos uint32
		latestXID uint64 = 10

		allEvents = make([]*replication.BinlogEvent, 0, 50)
		allData   bytes.Buffer
	)

	// use a binlog event generator to generate some binlog events.
	g, err := event.NewGenerator(flavor, serverID, latestPos, latestGTID1, previousGTIDSet, latestXID)
	require.NoError(t, err)

	// file header with FormatDescriptionEvent and PreviousGTIDsEvent
	events, data, err := g.GenFileHeader(0)
	require.NoError(t, err)
	allEvents = append(allEvents, events...)
	allData.Write(data)

	// CREATE DATABASE/TABLE, 3 DDL
	queries := []string{
		"CREATE DATABASE `db`",
		"COMMIT",
		"CREATE TABLE `db`.`tbl1` (c1 INT)",
		"CREATE TABLE `db`.`tbl2` (c1 INT)",
	}
	for _, query := range queries {
		events, data, err = g.GenDDLEvents("db", query, 0)
		require.NoError(t, err)
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
		events, data, err = g.GenDMLEvents(eventType, dmlData, 0)
		require.NoError(t, err)
		allEvents = append(allEvents, events...)
		allData.Write(data)
	}

	return g, allEvents, allData.Bytes()
}

func TestHandleEvent(t *testing.T) {
	// NOTE: we can test metrics later.
	var (
		reader2  = &mockReader{}
		parser2  = parser.New()
		writer2  = &mockWriter{}
		relayCfg = newRelayCfg(t, gmysql.MariaDBFlavor)
		r        = NewRelay(relayCfg).(*Relay)

		eventHeader = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
		}
		binlogPos       = gmysql.Position{Name: "mysql-bin.666888", Pos: 4}
		rotateEv, _     = event.GenRotateEvent(eventHeader, 123, []byte(binlogPos.Name), uint64(binlogPos.Pos))
		fakeRotateEv, _ = event.GenRotateEvent(eventHeader, 0, []byte(binlogPos.Name), uint64(1234))
		queryEv, _      = event.GenQueryEvent(eventHeader, 123, 0, 0, 0, nil, nil, []byte("CREATE DATABASE db_relay_test"))
	)

	r.writer = writer2

	cfg := getDBConfigForTest()
	conn.InitMockDB(t)
	db, err := conn.GetUpstreamDB(cfg)
	require.NoError(t, err)
	r.db = db
	require.NoError(t, r.Init(context.Background()))
	// NOTE: we can mock meta later.
	require.NoError(t, r.meta.Load())
	require.Nil(t, r.meta.AddDir("24ecd093-8cec-11e9-aa0d-0242ac170002", nil, nil, 0))

	// attach GTID sets to QueryEv
	queryEv2 := queryEv.Event.(*replication.QueryEvent)
	queryEv2.GSet, _ = gmysql.ParseGTIDSet(relayCfg.Flavor, "1-2-3")

	// reader return with an error
	for _, reader2.err = range []error{
		errors.New("reader error for testing"),
		replication.ErrChecksumMismatch,
		replication.ErrSyncClosed,
		replication.ErrNeedSyncAgain,
	} {
		handleErr := r.handleEvents(context.Background(), reader2, parser2)
		require.Equal(t, reader2.err, errors.Cause(handleErr))
	}

	// reader return fake rotate event
	reader2.err = nil
	reader2.result.Event = fakeRotateEv
	// writer return error to force handleEvents return
	writer2.err = errors.New("writer error for testing")
	// return with the annotated writer error
	err = r.handleEvents(context.Background(), reader2, parser2)
	require.Equal(t, writer2.err, errors.Cause(err))
	// after handle rotate event, we save and flush the meta immediately
	require.Equal(t, false, r.meta.Dirty())
	{
		lm := r.meta.(*LocalMeta)
		require.Equal(t, "mysql-bin.666888", lm.BinLogName)
		require.Equal(t, uint32(1234), lm.BinLogPos)
		filename := filepath.Join(lm.baseDir, lm.currentSubDir, utils.MetaFilename)
		lm2 := &LocalMeta{}
		_, err2 := toml.DecodeFile(filename, lm2)
		require.Nil(t, err2)
		require.Equal(t, "mysql-bin.666888", lm2.BinLogName)
		require.Equal(t, uint32(1234), lm2.BinLogPos)
	}
	{
		lm := r.meta.(*LocalMeta)
		backupUUID := lm.currentSubDir
		lm.currentSubDir = "not exist"
		err = r.handleEvents(context.Background(), reader2, parser2)
		require.Equal(t, true, os.IsNotExist(errors.Cause(err)))
		lm.currentSubDir = backupUUID
	}

	// reader return valid event
	reader2.err = nil
	reader2.result.Event = rotateEv

	// writer return error
	writer2.err = errors.New("writer error for testing")
	// return with the annotated writer error
	err = r.handleEvents(context.Background(), reader2, parser2)
	require.Equal(t, writer2.err, errors.Cause(err))
	require.Equal(t, false, r.meta.Dirty())

	// writer without error
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	writer2.err = nil
	err = r.handleEvents(ctx, reader2, parser2) // returned when ctx timeout
	require.Equal(t, ctx.Err(), errors.Cause(err))
	// check written event
	require.Equal(t, reader2.result.Event, writer2.latestEvent)
	// check meta
	_, pos := r.meta.Pos()
	_, gs := r.meta.GTID()
	require.Equal(t, binlogPos, pos)
	require.Equal(t, "", gs.String()) // no GTID sets in event yet

	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel2()

	// write a QueryEvent with GTID sets
	reader2.result.Event = queryEv
	err = r.handleEvents(ctx2, reader2, parser2)
	require.Equal(t, ctx.Err(), errors.Cause(err))
	// check written event
	require.Equal(t, reader2.result.Event, writer2.latestEvent)
	// check meta
	_, pos = r.meta.Pos()
	_, gs = r.meta.GTID()
	require.Equal(t, binlogPos.Name, pos.Name)
	require.Equal(t, queryEv.Header.LogPos, pos.Pos)
	require.Equal(t, queryEv2.GSet, gs) // got GTID sets

	// transformer return ignorable for the event
	reader2.err = nil
	reader2.result.Event = &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.HEARTBEAT_EVENT},
		Event:  &replication.GenericEvent{},
	}
	ctx4, cancel4 := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel4()
	err = r.handleEvents(ctx4, reader2, parser2)
	require.Equal(t, ctx.Err(), errors.Cause(err))
	select {
	case <-ctx4.Done():
	default:
		t.Fatalf("ignorable event for transformer not ignored")
	}

	// writer return ignorable for the event
	reader2.result.Event = queryEv
	writer2.result.Ignore = true
	ctx5, cancel5 := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel5()
	err = r.handleEvents(ctx5, reader2, parser2)
	require.Equal(t, ctx.Err(), errors.Cause(err))
	select {
	case <-ctx5.Done():
	default:
		t.Fatalf("ignorable event for writer not ignored")
	}
}

func TestReSetupMeta(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), conn.DefaultDBTimeout)
	defer cancel()

	var (
		relayCfg = newRelayCfg(t, gmysql.MySQLFlavor)
		r        = NewRelay(relayCfg).(*Relay)
	)
	cfg := getDBConfigForTest()
	mockDB := conn.InitMockDB(t)
	db, err := conn.GetUpstreamDB(cfg)
	require.NoError(t, err)
	r.db = db
	require.NoError(t, r.Init(context.Background()))

	// empty metadata
	require.NoError(t, r.meta.Load())
	verifyMetadata(t, r, "", minCheckpoint, "", nil)

	// open connected DB and get its UUID
	defer func() {
		r.db.Close()
		r.db = nil
	}()
	mockGetServerUUID(mockDB)
	uuid, err := conn.GetServerUUID(tcontext.NewContext(ctx, log.L()), r.db, r.cfg.Flavor)
	require.NoError(t, err)

	// re-setup meta with start pos adjusted
	r.cfg.EnableGTID = true
	r.cfg.BinlogGTID = "24ecd093-8cec-11e9-aa0d-0242ac170002:1-23"
	r.cfg.BinLogName = "mysql-bin.000005"

	require.NoError(t, r.setSyncConfig())
	// all adjusted gset should be empty since we didn't flush logs
	emptyGTID, err := gtid.ParserGTID(r.cfg.Flavor, "")
	require.NoError(t, err)

	mockGetServerUUID(mockDB)
	mockGetRandomServerID(mockDB)
	//  mock AddGSetWithPurged
	mockDB.ExpectQuery("select @@GLOBAL.gtid_purged").WillReturnRows(sqlmock.NewRows([]string{"@@GLOBAL.gtid_purged"}).AddRow(""))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tiflow/dm/pkg/binlog/reader/MockGetEmptyPreviousGTIDFromGTIDSet", "return()"))
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/pkg/binlog/reader/MockGetEmptyPreviousGTIDFromGTIDSet")
	require.NoError(t, r.reSetupMeta(ctx))
	uuid001 := fmt.Sprintf("%s.000001", uuid)
	verifyMetadata(t, r, uuid001, gmysql.Position{Name: r.cfg.BinLogName, Pos: 4}, emptyGTID.String(), []string{uuid001})

	// re-setup meta again, often happen when connecting a server behind a VIP.
	mockGetServerUUID(mockDB)
	mockGetRandomServerID(mockDB)
	mockDB.ExpectQuery("select @@GLOBAL.gtid_purged").WillReturnRows(sqlmock.NewRows([]string{"@@GLOBAL.gtid_purged"}).AddRow(""))
	require.NoError(t, r.reSetupMeta(ctx))
	uuid002 := fmt.Sprintf("%s.000002", uuid)
	verifyMetadata(t, r, uuid002, minCheckpoint, emptyGTID.String(), []string{uuid001, uuid002})

	r.cfg.BinLogName = "mysql-bin.000002"
	r.cfg.BinlogGTID = "24ecd093-8cec-11e9-aa0d-0242ac170002:1-50,24ecd093-8cec-11e9-aa0d-0242ac170003:1-50"
	r.cfg.UUIDSuffix = 2
	mockGetServerUUID(mockDB)
	mockGetRandomServerID(mockDB)
	mockDB.ExpectQuery("select @@GLOBAL.gtid_purged").WillReturnRows(sqlmock.NewRows([]string{"@@GLOBAL.gtid_purged"}).AddRow(""))
	require.NoError(t, r.reSetupMeta(ctx))
	verifyMetadata(t, r, uuid002, gmysql.Position{Name: r.cfg.BinLogName, Pos: 4}, emptyGTID.String(), []string{uuid002})

	// re-setup meta again, often happen when connecting a server behind a VIP.
	mockGetServerUUID(mockDB)
	mockGetRandomServerID(mockDB)
	mockDB.ExpectQuery("select @@GLOBAL.gtid_purged").WillReturnRows(sqlmock.NewRows([]string{"@@GLOBAL.gtid_purged"}).AddRow(""))
	require.NoError(t, r.reSetupMeta(ctx))
	uuid003 := fmt.Sprintf("%s.000003", uuid)
	verifyMetadata(t, r, uuid003, minCheckpoint, emptyGTID.String(), []string{uuid002, uuid003})
	require.NoError(t, mockDB.ExpectationsWereMet())
}

func verifyMetadata(t *testing.T, r *Relay, uuidExpected string,
	posExpected gmysql.Position, gsStrExpected string, uuidsExpected []string,
) {
	uuid, pos := r.meta.Pos()
	_, gs := r.meta.GTID()
	gsExpected, err := gtid.ParserGTID(gmysql.MySQLFlavor, gsStrExpected)
	require.NoError(t, err)
	require.Equal(t, uuidExpected, uuid)
	require.Equal(t, posExpected, pos)
	require.True(t, gs.Equal(gsExpected))

	indexFile := filepath.Join(r.cfg.RelayDir, utils.UUIDIndexFilename)
	UUIDs, err := utils.ParseUUIDIndex(indexFile)
	require.NoError(t, err)
	require.Equal(t, uuidsExpected, UUIDs)
}

func TestPreprocessEvent(t *testing.T) {
	type Case struct {
		event  *replication.BinlogEvent
		result preprocessResult
	}
	relay := &Relay{}
	parser2 := parser.New()
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  11,
			Flags:     0x01,
		}
		latestPos  uint32 = 456789
		gtidStr           = "9f61c5f9-1eef-11e9-b6cf-0242ac140003:5"
		gtidSet, _        = gtid.ParserGTID(gmysql.MySQLFlavor, gtidStr)
		schema            = []byte("test_schema")
		cases             = make([]Case, 0, 10)
	)

	// RotateEvent
	nextLogName := "mysql-bin.000123"
	position := uint64(4)
	ev, err := event.GenRotateEvent(header, latestPos, []byte(nextLogName), position)
	require.NoError(t, err)
	cases = append(cases, Case{
		event: ev,
		result: preprocessResult{
			LogPos:      uint32(position),
			NextLogName: nextLogName,
		},
	})

	// fake RotateEvent with zero timestamp
	header.Timestamp = 0
	ev, err = event.GenRotateEvent(header, latestPos, []byte(nextLogName), position)
	require.NoError(t, err)
	cases = append(cases, Case{
		event: ev,
		result: preprocessResult{
			LogPos:      uint32(position),
			NextLogName: nextLogName,
		},
	})
	header.Timestamp = uint32(time.Now().Unix()) // set to non-zero

	// fake RotateEvent with zero logPos
	fakeRotateHeader := *header
	ev, err = event.GenRotateEvent(&fakeRotateHeader, latestPos, []byte(nextLogName), position)
	require.NoError(t, err)
	ev.Header.LogPos = 0 // set to zero
	cases = append(cases, Case{
		event: ev,
		result: preprocessResult{
			LogPos:      uint32(position),
			NextLogName: nextLogName,
		},
	})

	// QueryEvent for DDL
	query := []byte("CREATE TABLE test_tbl (c1 INT)")
	ev, err = event.GenQueryEvent(header, latestPos, 0, 0, 0, nil, schema, query)
	require.NoError(t, err)
	ev.Event.(*replication.QueryEvent).GSet = gtidSet // set GTIDs manually
	cases = append(cases, Case{
		event: ev,
		result: preprocessResult{
			LogPos:      ev.Header.LogPos,
			GTIDSet:     gtidSet,
			CanSaveGTID: true,
		},
	})

	// QueryEvent for non-DDL
	query = []byte("BEGIN")
	ev, err = event.GenQueryEvent(header, latestPos, 0, 0, 0, nil, schema, query)
	require.NoError(t, err)
	cases = append(cases, Case{
		event: ev,
		result: preprocessResult{
			LogPos: ev.Header.LogPos,
		},
	})

	// XIDEvent
	xid := uint64(135)
	ev, err = event.GenXIDEvent(header, latestPos, xid)
	require.NoError(t, err)
	ev.Event.(*replication.XIDEvent).GSet = gtidSet // set GTIDs manually
	cases = append(cases, Case{
		event: ev,
		result: preprocessResult{
			LogPos:      ev.Header.LogPos,
			GTIDSet:     gtidSet,
			CanSaveGTID: true,
		},
	})

	// GenericEvent, non-HEARTBEAT_EVENT
	ev = &replication.BinlogEvent{Header: header, Event: &replication.GenericEvent{}}
	cases = append(cases, Case{
		event: ev,
		result: preprocessResult{
			LogPos: ev.Header.LogPos,
		},
	})

	// GenericEvent, HEARTBEAT_EVENT
	genericHeader := *header
	ev = &replication.BinlogEvent{Header: &genericHeader, Event: &replication.GenericEvent{}}
	ev.Header.EventType = replication.HEARTBEAT_EVENT
	cases = append(cases, Case{
		event: ev,
		result: preprocessResult{
			Ignore:       true,
			IgnoreReason: ignoreReasonHeartbeat,
			LogPos:       ev.Header.LogPos,
		},
	})

	// other event type without LOG_EVENT_ARTIFICIAL_F
	ev, err = event.GenCommonGTIDEvent(gmysql.MySQLFlavor, header.ServerID, latestPos, gtidSet, false, 0)
	require.NoError(t, err)
	cases = append(cases, Case{
		event: ev,
		result: preprocessResult{
			LogPos: ev.Header.LogPos,
		},
	})

	// other event type with LOG_EVENT_ARTIFICIAL_F
	ev, err = event.GenTableMapEvent(header, latestPos, 0, []byte("testdb"), []byte("testtbl"), []byte("INT"))
	require.NoError(t, err)
	ev.Header.Flags |= replication.LOG_EVENT_ARTIFICIAL_F
	cases = append(cases, Case{
		event: ev,
		result: preprocessResult{
			Ignore:       true,
			IgnoreReason: ignoreReasonArtificialFlag,
			LogPos:       ev.Header.LogPos,
		},
	})

	for _, cs := range cases {
		require.Equal(t, cs.result, relay.preprocessEvent(cs.event, parser2))
	}
}

func TestRecoverMySQL(t *testing.T) {
	var (
		relayDir           = t.TempDir()
		filename           = "test-mysql-bin.000001"
		parser2            = parser.New()
		flavor             = gmysql.MySQLFlavor
		previousGTIDSetStr = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,406a3f61-690d-11e7-87c5-6c92bf46f384:123-456,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,686e1ab6-c47e-11e7-a42c-6c92bf46f384:234-567"
		latestGTIDStr1     = "3ccc475b-2343-11e7-be21-6c0b84d59f30:14"
		latestGTIDStr2     = "53bfca22-690d-11e7-8a62-18ded7a37b78:495"
	)

	r := NewRelay(&Config{Flavor: flavor}).(*Relay)

	// different SIDs in GTID set
	previousGTIDSet, err := gtid.ParserGTID(flavor, previousGTIDSetStr)
	require.NoError(t, err)
	latestGTID1, err := gtid.ParserGTID(flavor, latestGTIDStr1)
	require.NoError(t, err)
	latestGTID2, err := gtid.ParserGTID(flavor, latestGTIDStr2)
	require.NoError(t, err)

	// generate binlog events
	g, _, baseData := genBinlogEventsWithGTIDs(t, flavor, previousGTIDSet, latestGTID1, latestGTID2)

	// expected latest pos/GTID set
	expectedPos := gmysql.Position{Name: filename, Pos: uint32(len(baseData))}
	// 3 DDL + 10 DML
	expectedGTIDsStr := "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-18,53bfca22-690d-11e7-8a62-18ded7a37b78:1-505,406a3f61-690d-11e7-87c5-6c92bf46f384:123-456,686e1ab6-c47e-11e7-a42c-6c92bf46f384:234-567"
	expectedGTIDs, err := gtid.ParserGTID(flavor, expectedGTIDsStr)
	require.NoError(t, err)

	// write the events to a file
	fullName := filepath.Join(relayDir, filename)
	err = os.WriteFile(fullName, baseData, 0o644)
	require.NoError(t, err)

	// try recover, but in fact do nothing
	result, err := r.doRecovering(context.Background(), relayDir, filename, parser2)
	require.NoError(t, err)
	require.False(t, result.Truncated)
	require.Equal(t, expectedPos, result.LatestPos)
	require.Equal(t, expectedGTIDs, result.LatestGTIDs)

	// check file size, whether no recovering operation applied
	fs, err := os.Stat(fullName)
	require.NoError(t, err)
	require.Equal(t, int64(len(baseData)), fs.Size())

	// generate another transaction, DDL
	extraEvents, extraData, err := g.GenDDLEvents("db2", "CREATE DATABASE db2", 0)
	require.NoError(t, err)
	require.Len(t, extraEvents, 2) // [GTID, Query]

	// write an incomplete event to the file
	corruptData := extraEvents[0].RawData[:len(extraEvents[0].RawData)-2]
	f, err := os.OpenFile(fullName, os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	_, err = f.Write(corruptData)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// check file size, increased
	fs, err = os.Stat(fullName)
	require.NoError(t, err)
	require.Equal(t, int64(len(baseData)+len(corruptData)), fs.Size())

	// try recover, truncate the incomplete event
	result, err = r.doRecovering(context.Background(), relayDir, filename, parser2)
	require.NoError(t, err)
	require.True(t, result.Truncated)
	require.Equal(t, expectedPos, result.LatestPos)
	require.Equal(t, expectedGTIDs, result.LatestGTIDs)

	// check file size, truncated
	fs, err = os.Stat(fullName)
	require.NoError(t, err)
	require.Equal(t, int64(len(baseData)), fs.Size())

	// write an incomplete transaction
	f, err = os.OpenFile(fullName, os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	var extraLen int64
	for i := 0; i < len(extraEvents)-1; i++ {
		_, err = f.Write(extraEvents[i].RawData)
		require.NoError(t, err)
		extraLen += int64(len(extraEvents[i].RawData))
	}
	require.NoError(t, f.Close())

	// check file size, increased
	fs, err = os.Stat(fullName)
	require.NoError(t, err)
	require.Equal(t, int64(len(baseData))+extraLen, fs.Size())

	// try recover, truncate the incomplete transaction
	result, err = r.doRecovering(context.Background(), relayDir, filename, parser2)
	require.NoError(t, err)
	require.True(t, result.Truncated)
	require.Equal(t, expectedPos, result.LatestPos)
	require.Equal(t, expectedGTIDs, result.LatestGTIDs)

	// check file size, truncated
	fs, err = os.Stat(fullName)
	require.NoError(t, err)
	require.Equal(t, int64(len(baseData)), fs.Size())

	// write an completed transaction
	f, err = os.OpenFile(fullName, os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	for i := 0; i < len(extraEvents); i++ {
		_, err = f.Write(extraEvents[i].RawData)
		require.NoError(t, err)
	}
	require.NoError(t, f.Close())

	// check file size, increased
	fs, err = os.Stat(fullName)
	require.NoError(t, err)
	require.Equal(t, int64(len(baseData)+len(extraData)), fs.Size())

	// try recover, no operation applied
	expectedPos.Pos += uint32(len(extraData))
	// 4 DDL + 10 DML
	expectedGTIDsStr = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-18,53bfca22-690d-11e7-8a62-18ded7a37b78:1-506,406a3f61-690d-11e7-87c5-6c92bf46f384:123-456,686e1ab6-c47e-11e7-a42c-6c92bf46f384:234-567"
	expectedGTIDs, err = gtid.ParserGTID(flavor, expectedGTIDsStr)
	require.NoError(t, err)
	result, err = r.doRecovering(context.Background(), relayDir, filename, parser2)
	require.NoError(t, err)
	require.False(t, result.Truncated)
	require.Equal(t, expectedPos, result.LatestPos)
	require.Equal(t, expectedGTIDs, result.LatestGTIDs)

	// compare file data
	var allData bytes.Buffer
	allData.Write(baseData)
	allData.Write(extraData)
	fileData, err := os.ReadFile(fullName)
	require.NoError(t, err)
	require.Equal(t, allData.Bytes(), fileData)
}

func TestRecoverMySQLNone(t *testing.T) {
	relayDir := t.TempDir()
	parser2 := parser.New()

	r := NewRelay(&Config{Flavor: gmysql.MySQLFlavor}).(*Relay)

	// no file specified to recover
	result, err := r.doRecovering(context.Background(), relayDir, "", parser2)
	require.NoError(t, err)
	require.False(t, result.Truncated)

	filename := "mysql-bin.000001"

	// file not exist, no need to recover
	result, err = r.doRecovering(context.Background(), relayDir, filename, parser2)
	require.NoError(t, err)
	require.False(t, result.Truncated)
}
