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

package logutil

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/pingcap/tiflow/dm/pkg/encrypt"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestHideSensitive(t *testing.T) {
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)

	t.Cleanup(func() {
		encrypt.InitCipher(nil)
	})
	encrypt.InitCipher(key)
	encryptedPass, err := utils.Encrypt("this is password")
	require.NoError(t, err)

	strs := []struct {
		old string
		new string
	}{
		{ // operate source
			fmt.Sprintf(`from:\n  host: 127.0.0.1\n  user: root\n  password: '%s'\n  port: 3306\n`, encryptedPass),
			`from:\n  host: 127.0.0.1\n  user: root\n  password: ******\n  port: 3306\n`,
		}, { // operate source empty password
			`from:\n  host: 127.0.0.1\n  user: root\n  password: \n  port: 3306\n`,
			`from:\n  host: 127.0.0.1\n  user: root\n  password: ******\n  port: 3306\n`,
		}, { // start task
			fmt.Sprintf(`\n\ntarget-database:\n  host: \"127.0.0.1\"\n  port: 4000\n  user: \"test\"\n  password: \"%s\"\n\nmysql-instances:\n  - source-id: \"mysql-replica-01\"\n`, encryptedPass),
			`\n\ntarget-database:\n  host: \"127.0.0.1\"\n  port: 4000\n  user: \"test\"\n  password: \"******\"\n\nmysql-instances:\n  - source-id: \"mysql-replica-01\"\n`,
		}, { // start task empty passowrd
			`\n\ntarget-database:\n  host: \"127.0.0.1\"\n  port: 4000\n  user: \"test\"\n  password: \"\"\n\nmysql-instances:\n  - source-id: \"mysql-replica-01\"\n`,
			`\n\ntarget-database:\n  host: \"127.0.0.1\"\n  port: 4000\n  user: \"test\"\n  password: \"******\"\n\nmysql-instances:\n  - source-id: \"mysql-replica-01\"\n`,
		}, { // operate source
			fmt.Sprintf(`user: root\n  password: '%s'\n  port: 3306 security:\n ssl-ca-bytes:\n    - 45\n    ssl-key-bytes:\n    - 45\n    ssl-cert-bytes:\n    - 45\npurge:`, encryptedPass),
			`user: root\n  password: ******\n  port: 3306 security:\n ssl-ca-bytes: "******"\n    ssl-key-bytes: "******"\n    ssl-cert-bytes: "******"\npurge:`,
		}, { // start task with ssl
			`\n\ntarget-database:\n  host: \"127.0.0.1\"\n  port: 4000\n  user: \"test\"\n  password: \"\"\n security:\n ssl-ca-bytes:\n    - 45\n    ssl-key-bytes:\n    - 45\n    ssl-cert-bytes:\n    - 45\nmysql-instances:\n  - source-id: \"mysql-replica-01\"\n`,
			`\n\ntarget-database:\n  host: \"127.0.0.1\"\n  port: 4000\n  user: \"test\"\n  password: \"******\"\n security:\n ssl-ca-bytes: "******"\n    ssl-key-bytes: "******"\n    ssl-cert-bytes: "******"\nmysql-instances:\n  - source-id: \"mysql-replica-01\"\n`,
		}, { // engine dm job with ssl
			`c="id:\"test_job\" config:\"ssl-ca-bytes:  -----BEGIN CERTIFICATE-----\\nrandom1\\nrandom2\\nrandom3\\n-----END CERTIFICATE-----\\nssl-key-bytes: '-----BEGIN PRIVATE KEY-----\\nrandom1\\nrandom2\\n-----END PRIVATE KEY-----'\\nssl-cert-bytes:  \\\"-----BEGIN CERTIFICATE REQUEST-----\\nrandom1\\nrandom2\\nrandom3\\n-----END CERTIFICATE REQUEST-----\\\"\""`,
			`c="id:\"test_job\" config:\"ssl-ca-bytes: "******"\\nssl-key-bytes: "******"\\nssl-cert-bytes: "******"\\\"\""`,
		},
	}
	for _, str := range strs {
		require.Equal(t, str.new, HideSensitive(str.old))
	}
}
