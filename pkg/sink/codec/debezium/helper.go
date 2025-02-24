// Copyright 2024 PingCAP, Inc.
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

package debezium

import (
	"encoding/binary"
	"fmt"

	"github.com/pingcap/tiflow/pkg/sink/codec/common"
)

func parseBit(s string, n int) string {
	var result string
	if len(s) > 0 {
		// Leading zeros may be omitted
		result = fmt.Sprintf("%0*b", n%8, s[0])
	}
	for i := 1; i < len(s); i++ {
		result += fmt.Sprintf("%08b", s[i])
	}
	return result
}

func getBitFromUint64(n int, v uint64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	numBytes := n / 8
	if n%8 != 0 {
		numBytes += 1
	}
	return buf[:numBytes]
}

func getSchemaTopicName(namespace string, schema string, table string) string {
	return fmt.Sprintf("%s.%s.%s",
		common.SanitizeName(namespace),
		common.SanitizeName(schema),
		common.SanitizeTopicName(table))
}
