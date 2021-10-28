// Copyright 2021 PingCAP, Inc.
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

package loader

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
)

var _ = Suite(&testLoaderSuite{})

type testLoaderSuite struct{}

func (*testLoaderSuite) TestJobQueue(c *C) {
	procedure := func(ctx context.Context, jobsCount int, handler func(ctx context.Context, job *restoreSchemaJob) error) error {
		jobQueue := newJobQueue(ctx, 16, 16)
		jobQueue.startConsumers(handler)
		for i := 0; i < jobsCount; i++ {
			job := &restoreSchemaJob{
				session: &DBConn{}, // just for testing
			}
			if i == jobsCount/2 {
				job.database = "error"
			}
			err := jobQueue.push(job)
			if err != nil {
				runtimeErr := jobQueue.close()
				if errors.ErrorEqual(err, context.Canceled) {
					err = runtimeErr
				}
				return err
			}
		}
		return jobQueue.close()
	}

	injectErr := errors.New("random injected error")
	cases := []struct {
		ctx         context.Context
		jobsCount   int
		handler     func(ctx context.Context, job *restoreSchemaJob) error
		exceptedErr error
	}{
		{
			ctx:       context.Background(),
			jobsCount: 128,
			handler: func(ctx context.Context, job *restoreSchemaJob) error {
				if job.database == "error" {
					return injectErr
				}
				return nil
			},
			exceptedErr: injectErr,
		},
		{
			ctx:       context.Background(),
			jobsCount: 128,
			handler: func(ctx context.Context, job *restoreSchemaJob) error {
				return nil
			},
			exceptedErr: nil,
		},
	}

	for _, testcase := range cases {
		err := procedure(testcase.ctx, testcase.jobsCount, testcase.handler)
		c.Assert(err, Equals, testcase.exceptedErr)
	}
}
