package owner

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/orchestrator"
)

var _ = check.Suite(&schedulerSuite{})

type schedulerSuite struct {
	changefeedId model.ChangeFeedID
	state        *model.ChangefeedReactorState
	tester       *orchestrator.ReactorStateTester
	captures     map[model.CaptureID]*model.CaptureInfo
	scheduler    *scheduler
}

func (s *schedulerSuite) reset(c *check.C) {
	s.changefeedId = fmt.Sprintf("test-changefeed-%x", rand.Uint32())
	s.state = model.NewChangefeedReactorState("test-changefeed")
	s.tester = orchestrator.NewReactorStateTester(c, s.state, nil)
	s.scheduler = newScheduler()
	s.captures = make(map[model.CaptureID]*model.CaptureInfo)
	s.state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		return &model.ChangeFeedStatus{}, true, nil
	})
	s.tester.MustApplyPatches()
}

func (s *schedulerSuite) addCapture(captureID model.CaptureID) {
	captureInfo := &model.CaptureInfo{
		ID: captureID,
	}
	s.captures[captureID] = captureInfo
	s.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		return &model.TaskStatus{}, true, nil
	})
	s.tester.MustApplyPatches()
}

func (s *schedulerSuite) removeCapture(captureID model.CaptureID) {
	delete(s.captures, captureID)
}

func (s *schedulerSuite) TestScheduleOneCapture(c *check.C) {
	s.reset(c)
	captureID := "test-capture-1"
	s.addCapture(captureID)

	// add three tables
	allTableInListened := s.scheduler.Tick(s.state, []model.TableID{1, 2, 3, 4}, s.captures)
	c.Assert(allTableInListened, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		1: {StartTs: 1}, 2: {StartTs: 1}, 3: {StartTs: 1}, 4: {StartTs: 1},
	})
	c.Assert(s.state.TaskStatuses[captureID].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{
		1: {Done: false, Delete: false, BoundaryTs: 1, Status: model.OperDispatched},
		2: {Done: false, Delete: false, BoundaryTs: 1, Status: model.OperDispatched},
		3: {Done: false, Delete: false, BoundaryTs: 1, Status: model.OperDispatched},
		4: {Done: false, Delete: false, BoundaryTs: 1, Status: model.OperDispatched},
	})
	allTableInListened = s.scheduler.Tick(s.state, []model.TableID{1, 2, 3, 4}, s.captures)
	c.Assert(allTableInListened, check.IsTrue)
	s.tester.MustApplyPatches()

	// two tables finish adding operation
	s.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.Operation[2].Done = true
		status.Operation[2].Status = model.OperFinished
		status.Operation[3].Done = true
		status.Operation[3].Status = model.OperFinished
		return status, true, nil
	})
	s.tester.MustApplyPatches()

	// remove table 1,2 and add table 4,5
	allTableInListened = s.scheduler.Tick(s.state, []model.TableID{3, 4, 5}, s.captures)
	c.Assert(allTableInListened, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		3: {StartTs: 1}, 4: {StartTs: 1}, 5: {StartTs: 1},
	})
	c.Assert(s.state.TaskStatuses[captureID].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{
		1: {Done: false, Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
		2: {Done: false, Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
		4: {Done: false, Delete: false, BoundaryTs: 1, Status: model.OperDispatched},
		5: {Done: false, Delete: false, BoundaryTs: 1, Status: model.OperDispatched},
	})

	// move a non exist table to a non exist capture
	s.scheduler.MoveTable(2, "fake-capture")
	// move tables to a non exist capture
	s.scheduler.MoveTable(3, "fake-capture")
	s.scheduler.MoveTable(4, "fake-capture")
	allTableInListened = s.scheduler.Tick(s.state, []model.TableID{3, 4, 5}, s.captures)
	c.Assert(allTableInListened, check.IsTrue)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		4: {StartTs: 1}, 5: {StartTs: 1},
	})
	c.Assert(s.state.TaskStatuses[captureID].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{
		1: {Done: false, Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
		2: {Done: false, Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
		3: {Done: false, Delete: true, BoundaryTs: 0, Status: model.OperDispatched},
		4: {Done: false, Delete: false, BoundaryTs: 1, Status: model.OperDispatched},
		5: {Done: false, Delete: false, BoundaryTs: 1, Status: model.OperDispatched},
	})

	// finish all operations
	s.state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.Operation[1].Done = true
		status.Operation[1].Status = model.OperFinished
		status.Operation[2].Done = true
		status.Operation[2].Status = model.OperFinished
		status.Operation[3].Done = true
		status.Operation[3].Status = model.OperFinished
		status.Operation[4].Done = true
		status.Operation[4].Status = model.OperFinished
		status.Operation[5].Done = true
		status.Operation[5].Status = model.OperFinished
		return status, true, nil
	})
	s.tester.MustApplyPatches()

	allTableInListened = s.scheduler.Tick(s.state, []model.TableID{3, 4, 5}, s.captures)
	c.Assert(allTableInListened, check.IsTrue)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		4: {StartTs: 1}, 5: {StartTs: 1},
	})
	c.Assert(s.state.TaskStatuses[captureID].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{})

	// table 3 is missing by expected, because the table was trying to move to a invalid capture
	// and the move will failed, the table 3 will be add in next tick
	allTableInListened = s.scheduler.Tick(s.state, []model.TableID{3, 4, 5}, s.captures)
	c.Assert(allTableInListened, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		4: {StartTs: 1}, 5: {StartTs: 1},
	})
	c.Assert(s.state.TaskStatuses[captureID].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{})

	allTableInListened = s.scheduler.Tick(s.state, []model.TableID{3, 4, 5}, s.captures)
	c.Assert(allTableInListened, check.IsFalse)
	s.tester.MustApplyPatches()
	c.Assert(s.state.TaskStatuses[captureID].Tables, check.DeepEquals, map[model.TableID]*model.TableReplicaInfo{
		3: {StartTs: 1}, 4: {StartTs: 1}, 5: {StartTs: 1},
	})
	c.Assert(s.state.TaskStatuses[captureID].Operation, check.DeepEquals, map[model.TableID]*model.TableOperation{
		3: {Done: false, Delete: false, BoundaryTs: 1, Status: model.OperDispatched},
	})
}
