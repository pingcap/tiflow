package cluster

import (
	"context"
	"time"

	"github.com/hanfei1991/microcosm/pb"
)

// JobMasterClient defines master client from job master to server master
type JobMasterClient interface {
	RequestForSchedule(
		ctx context.Context,
		req *pb.TaskSchedulerRequest,
		timeout time.Duration,
	) (*pb.TaskSchedulerResponse, error)
}
