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

package clock

import (
	"time"

	bclock "github.com/benbjohnson/clock"
	"github.com/gavv/monotime"
)

type (
	Timer         = bclock.Timer
	Ticker        = bclock.Ticker
	MonotonicTime time.Duration
)

var unixEpoch = time.Unix(0, 0)

type Clock interface {
	bclock.Clock
	Mono() MonotonicTime
}

type withRealMono struct {
	bclock.Clock
}

func (r withRealMono) Mono() MonotonicTime {
	return MonotonicTime(monotime.Now())
}

type Mock struct {
	*bclock.Mock
}

func (r Mock) Mono() MonotonicTime {
	return MonotonicTime(r.Now().Sub(unixEpoch))
}

func New() Clock {
	return withRealMono{bclock.New()}
}

func NewMock() *Mock {
	return &Mock{bclock.NewMock()}
}

func (m MonotonicTime) Sub(other MonotonicTime) time.Duration {
	return time.Duration(m - other)
}

func MonoNow() MonotonicTime {
	return MonotonicTime(monotime.Now())
}

func ToMono(t time.Time) MonotonicTime {
	return MonotonicTime(t.Sub(unixEpoch))
}
