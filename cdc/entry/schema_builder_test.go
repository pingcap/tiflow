package entry

import (
	"context"
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/model"
)

type schemaBuilderSuite struct{}

var _ = Suite(&schemaBuilderSuite{})

func (s *schemaBuilderSuite) TestStorageBuilder(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	historyJobs := []*timodel.Job{{
		ID:       1,
		State:    timodel.JobStateSynced,
		SchemaID: 1,
		Type:     timodel.ActionCreateSchema,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 1,
			DBInfo: &timodel.DBInfo{
				ID:    1,
				Name:  timodel.NewCIStr("testDB"),
				State: timodel.StatePublic,
			}, FinishedTS: 10},
		Query: "create database testDB",
	}}
	ddlEventCh := make(chan *model.RawKVEntry)
	go func() {
		for i := 2; i < 50; i++ {
			job := buildCreateTableJob(int64(i), 1, int64(i-1), fmt.Sprintf("testTBL%d", i-1), uint64(i*10))
			ddlEventCh <- job2RawKvEntry(job)
		}
	}()
	b := NewStorageBuilder(historyJobs, ddlEventCh)
	go func() {
		err := b.Run(ctx)
		c.Assert(errors.Cause(err), Equals, context.Canceled)
	}()
	for i := 3; i < 10; i++ {
		storage, err := b.Build(uint64(i * 10))
		c.Assert(err, IsNil)
		for retry := 0; retry < 100; retry++ {
			err = storage.HandlePreviousDDLJobIfNeed(uint64(i * 10))
			if errors.Cause(err) == model.ErrUnresolved {
				time.Sleep(10 * time.Millisecond)
			}
		}
		c.Assert(err, IsNil)
		_, ok := storage.TableByID(int64(1))
		c.Assert(ok, IsTrue)
		_, ok = storage.TableByID(int64(2))
		c.Assert(ok, IsTrue)
		_, ok = storage.TableByID(int64(i - 1))
		c.Assert(ok, IsTrue)
		_, ok = storage.TableByID(int64(i - 2))
		c.Assert(ok, IsTrue)
		_, ok = storage.TableByID(int64(i))
		c.Assert(ok, IsFalse)
		err = b.DoGc(uint64(i * 10))
		c.Assert(err, IsNil)
	}

}

func buildCreateTableJob(jobID int64, schemaID int64, tableID int64, tableName string, finishedTs uint64) *timodel.Job {
	return &timodel.Job{
		ID:       jobID,
		State:    timodel.JobStateSynced,
		SchemaID: schemaID,
		TableID:  tableID,
		Type:     timodel.ActionCreateTable,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 2,
			TableInfo: &timodel.TableInfo{
				ID:    tableID,
				Name:  timodel.NewCIStr(tableName),
				State: timodel.StatePublic,
			}, FinishedTS: finishedTs},
		Query: "create table " + tableName,
	}
}

func job2RawKvEntry(job *timodel.Job) *model.RawKVEntry {
	value, err := job.Encode(false)
	if err != nil {
		panic(err)
	}
	return &model.RawKVEntry{
		OpType: model.OpTypePut,
		Key:    buildMetaKey([]byte(ddlJobListKey), job.ID),
		Value:  value,
		Ts:     job.BinlogInfo.FinishedTS,
	}
}

func (s *schemaBuilderSuite) TestJobList(c *C) {
	testCases := []struct {
		append             []*timodel.Job
		fetchTs            uint64
		expectedCurrentJob *timodel.Job
		expectedJobs       []*timodel.Job
		removeTs           uint64
		gcTs               uint64
	}{{append: []*timodel.Job{
		{ID: 1, BinlogInfo: &timodel.HistoryInfo{FinishedTS: 10}},
		{ID: 2, BinlogInfo: &timodel.HistoryInfo{FinishedTS: 20}},
		{ID: 3, BinlogInfo: &timodel.HistoryInfo{FinishedTS: 30}}},
		fetchTs:            0,
		expectedCurrentJob: nil,
		expectedJobs:       nil,
		removeTs:           0,
		gcTs:               0,
	}, {append: []*timodel.Job{
		{ID: 4, BinlogInfo: &timodel.HistoryInfo{FinishedTS: 40}},
		{ID: 5, BinlogInfo: &timodel.HistoryInfo{FinishedTS: 50}}},
		fetchTs:            25,
		expectedCurrentJob: &timodel.Job{ID: 2, BinlogInfo: &timodel.HistoryInfo{FinishedTS: 20}},
		expectedJobs: []*timodel.Job{
			{ID: 1, BinlogInfo: &timodel.HistoryInfo{FinishedTS: 10}},
			{ID: 2, BinlogInfo: &timodel.HistoryInfo{FinishedTS: 20}},
		},
		removeTs: 22,
		gcTs:     20,
	}, {append: []*timodel.Job{
		{ID: 6, BinlogInfo: &timodel.HistoryInfo{FinishedTS: 60}}},
		fetchTs:            55,
		expectedCurrentJob: &timodel.Job{ID: 5, BinlogInfo: &timodel.HistoryInfo{FinishedTS: 50}},
		expectedJobs: []*timodel.Job{
			{ID: 3, BinlogInfo: &timodel.HistoryInfo{FinishedTS: 30}},
			{ID: 4, BinlogInfo: &timodel.HistoryInfo{FinishedTS: 40}},
			{ID: 5, BinlogInfo: &timodel.HistoryInfo{FinishedTS: 50}},
		},
		removeTs: 52,
		gcTs:     50,
	}, {append: []*timodel.Job{},
		fetchTs:            80,
		expectedCurrentJob: &timodel.Job{ID: 6, BinlogInfo: &timodel.HistoryInfo{FinishedTS: 60}},
		expectedJobs: []*timodel.Job{
			{ID: 6, BinlogInfo: &timodel.HistoryInfo{FinishedTS: 60}},
		},
		removeTs: 80,
		gcTs:     60,
	}}
	l := newJobList()
	currentJobEle := l.Head()
	for _, tc := range testCases {
		l.AppendJob(tc.append...)
		var jobs []*timodel.Job
		currentJobEle, jobs = l.FetchNextJobs(currentJobEle, tc.fetchTs)
		c.Assert(jobs, DeepEquals, tc.expectedJobs)
		c.Assert(currentJobEle.Value, DeepEquals, tc.expectedCurrentJob)
		l.RemoveOverdueJobs(tc.removeTs)
		c.Assert(l.gcTs, Equals, tc.gcTs)
	}
}
