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

package election_test

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/tiflow/pkg/election"
	"github.com/pingcap/tiflow/pkg/election/mock"
	"github.com/stretchr/testify/require"
)

func TestConfigAdjustAndValidate(t *testing.T) {
	t.Parallel()

	err := (&election.Config{
		Name:    "name1",
		Storage: mock.NewMockStorage(gomock.NewController(t)),
		LeaderCallback: func(ctx context.Context) error {
			return nil
		},
	}).AdjustAndValidate()
	require.EqualError(t, err, "id must not be empty")

	err = (&election.Config{
		ID: "id1",
		LeaderCallback: func(ctx context.Context) error {
			return nil
		},
	}).AdjustAndValidate()
	require.EqualError(t, err, "storage must not be nil")

	err = (&election.Config{
		ID:      "id1",
		Name:    "name1",
		Storage: mock.NewMockStorage(gomock.NewController(t)),
	}).AdjustAndValidate()
	require.EqualError(t, err, "LeaderCallback must not be nil")

	err = (&election.Config{
		ID:      "id1",
		Name:    "name1",
		Storage: mock.NewMockStorage(gomock.NewController(t)),
		LeaderCallback: func(ctx context.Context) error {
			return nil
		},
		LeaseDuration: time.Second * 10,
		RenewInterval: time.Second * 20,
	}).AdjustAndValidate()
	require.EqualError(t, err, "RenewInterval must not be greater than LeaseDuration")

	err = (&election.Config{
		ID:      "id1",
		Name:    "name1",
		Storage: mock.NewMockStorage(gomock.NewController(t)),
		LeaderCallback: func(ctx context.Context) error {
			return nil
		},
		LeaseDuration: time.Second * 15,
		RenewInterval: time.Second * 10,
		RenewDeadline: time.Second * 10,
	}).AdjustAndValidate()
	require.EqualError(t, err, "RenewDeadline must not be greater than LeaseDuration - RenewInterval")
}
