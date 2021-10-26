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

package message

import (
	"encoding/json"
	"testing"

	"github.com/pingcap/ticdc/pkg/leakutil"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	leakutil.SetUpLeakTest(m)
}

// Make sure Message can be printed in JSON format, so that it can be logged by
// pingcap/log package.
func TestJSONPrint(t *testing.T) {
	_, err := json.Marshal(Message{})
	require.Nil(t, err)
}

func TestTickMessage(t *testing.T) {
	msg := TickMessage()
	require.Equal(t, TypeTick, msg.Tp)
}

func TestBarrierMessage(t *testing.T) {
	msg := BarrierMessage(1)
	require.Equal(t, TypeBarrier, msg.Tp)
}
