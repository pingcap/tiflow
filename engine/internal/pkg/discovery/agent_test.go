// Copyright 2022 PingCAP, Inc.
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

package discovery

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	pkgClient "github.com/pingcap/tiflow/engine/pkg/client"
	"github.com/stretchr/testify/require"
)

func TestAgent(t *testing.T) {
	t.Parallel()

	discoveryClient := pkgClient.NewMockServerMasterClient(gomock.NewController(t))
	agent := NewAgent(discoveryClient, time.Millisecond*100)

	var nodes sync.Map

	discoveryClient.EXPECT().
		ListMasters(gomock.Any()).AnyTimes().
		DoAndReturn(func(ctx context.Context) ([]*pb.Master, error) {
			var ret []*pb.Master
			nodes.Range(func(_, value any) bool {
				node := value.(Node)
				if node.Tp == NodeTypeMaster {
					ret = append(ret, &pb.Master{
						Id:      node.ID,
						Address: node.Addr,
					})
				}
				return true
			})
			return ret, nil
		})

	discoveryClient.EXPECT().
		ListExecutors(gomock.Any()).AnyTimes().
		DoAndReturn(func(ctx context.Context) ([]*pb.Executor, error) {
			var ret []*pb.Executor
			nodes.Range(func(_, value any) bool {
				node := value.(Node)
				if node.Tp == NodeTypeExecutor {
					ret = append(ret, &pb.Executor{
						Id:      node.ID,
						Address: node.Addr,
					})
				}
				return true
			})
			return ret, nil
		})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := agent.Run(ctx)
		require.ErrorIs(t, err, context.Canceled)
	}()

	initialSnap := Snapshot{
		"master-1": Node{
			Tp:   NodeTypeMaster,
			ID:   "master-1",
			Addr: "dummy-1:2222",
		},
		"executor-2": Node{
			Tp:   NodeTypeExecutor,
			ID:   "executor-2",
			Addr: "dummy-2:2222",
		},
		"executor-3": Node{
			Tp:   NodeTypeExecutor,
			ID:   "executor-3",
			Addr: "dummy-3:2222",
		},
	}

	for _, node := range initialSnap {
		nodes.Store(node.ID, node)
	}

	require.Eventually(t, func() bool {
		snap, receiver, err := agent.Subscribe(ctx)
		require.NoError(t, err)
		receiver.Close()
		return reflect.DeepEqual(snap, initialSnap)
	}, time.Second, time.Millisecond*100, "should get initial snapshot")

	_, receiver, err := agent.Subscribe(ctx)
	require.NoError(t, err)

	expectedEvents := []Event{
		{
			Tp: EventTypeAdd,
			Node: Node{
				Tp:   NodeTypeExecutor,
				ID:   "executor-4",
				Addr: "dummy-4:2222",
			},
		},
		{
			Tp: EventTypeAdd,
			Node: Node{
				Tp:   NodeTypeExecutor,
				ID:   "executor-5",
				Addr: "dummy-5:2222",
			},
		},
		{
			Tp: EventTypeDel,
			Node: Node{
				Tp:   NodeTypeExecutor,
				ID:   "executor-3",
				Addr: "dummy-3:2222",
			},
		},
		{
			Tp: EventTypeAdd,
			Node: Node{
				Tp:   NodeTypeMaster,
				ID:   "master-6",
				Addr: "dummy-6:2222",
			},
		},
		{
			Tp: EventTypeDel,
			Node: Node{
				Tp:   NodeTypeMaster,
				ID:   "master-1",
				Addr: "dummy-1:2222",
			},
		},
	}

	for _, expectedEvent := range expectedEvents {
		if expectedEvent.Tp == EventTypeAdd {
			nodes.Store(expectedEvent.Node.ID, expectedEvent.Node)
		} else {
			nodes.Delete(expectedEvent.Node.ID)
		}
		select {
		case <-ctx.Done():
			require.Failf(t, "unreachable", "%s", ctx.Err().Error())
		case event := <-receiver.C:
			require.Equal(t, expectedEvent, event)
		}
	}
	receiver.Close()

	require.Eventually(t, func() bool {
		snap, receiver, err := agent.Subscribe(ctx)
		require.NoError(t, err)
		receiver.Close()
		return reflect.DeepEqual(snap, Snapshot{
			"executor-2": Node{
				Tp:   NodeTypeExecutor,
				ID:   "executor-2",
				Addr: "dummy-2:2222",
			},
			"executor-4": Node{
				Tp:   NodeTypeExecutor,
				ID:   "executor-4",
				Addr: "dummy-4:2222",
			},
			"executor-5": Node{
				Tp:   NodeTypeExecutor,
				ID:   "executor-5",
				Addr: "dummy-5:2222",
			},
			"master-6": Node{
				Tp:   NodeTypeMaster,
				ID:   "master-6",
				Addr: "dummy-6:2222",
			},
		})
	}, time.Second, time.Millisecond*100, "should get expected snapshot")

	cancel()
	wg.Wait()
}
