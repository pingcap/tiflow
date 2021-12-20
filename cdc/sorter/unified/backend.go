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

package unified

import "github.com/pingcap/tiflow/cdc/model"

type backEnd interface {
	reader() (backEndReader, error)
	writer() (backEndWriter, error)
	free() error
}

type backEndReader interface {
	readNext() (*model.PolymorphicEvent, error)
	resetAndClose() error
}

type backEndWriter interface {
	writeNext(event *model.PolymorphicEvent) error
	writtenCount() int
	dataSize() uint64
	flushAndClose() error
}
