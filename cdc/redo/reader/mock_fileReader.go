//  Copyright 2021 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

// Code generated by mockery v0.0.0-dev. DO NOT EDIT.

package reader

import (
	model "github.com/pingcap/tiflow/cdc/model"
	mock "github.com/stretchr/testify/mock"
)

// mockFileReader is an autogenerated mock type for the fileReader type
type mockFileReader struct {
	mock.Mock
}

// Close provides a mock function with given fields:
func (_m *mockFileReader) Close() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Read provides a mock function with given fields: log
func (_m *mockFileReader) Read(log *model.RedoLog) error {
	ret := _m.Called(log)

	var r0 error
	if rf, ok := ret.Get(0).(func(*model.RedoLog) error); ok {
		r0 = rf(log)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
