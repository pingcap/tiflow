// Copyright 2020 PingCAP, Inc.
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

package avro

import (
	"github.com/pingcap/ticdc/integration/tests"
)

//nolint:unused
type unsignedCase struct {
	tests.UnsignedCase
}

// NewUnsignedCase create a test case to check the correction of unsigned integer
func NewUnsignedCase() *unsignedCase {
	return &unsignedCase{
		UnsignedCase: tests.UnsignedCase{
			Task: tests.NewUnsignedCase(tests.ProtocolAvro),
		},
	}
}
