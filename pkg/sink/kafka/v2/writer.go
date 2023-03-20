// Copyright 2023 PingCAP, Inc.
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

package v2

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// Writer is the interface of the kafka-go writer, it contains a subset of all methods which is used
// by the kafka sink.
// This interface is mainly used to support mock kafka-go client in unit test.
type Writer interface {
	Stats() kafka.WriterStats
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}
