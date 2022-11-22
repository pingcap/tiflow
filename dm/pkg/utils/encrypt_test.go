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

package utils

import (
	"encoding/base64"
	"testing"

	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

func TestEncrypt(t *testing.T) {
	plaintext := "abc@123"
	ciphertext, err := Encrypt(plaintext)
	require.NoError(t, err)

	plaintext2, err := Decrypt(ciphertext)
	require.NoError(t, err)
	require.Equal(t, plaintext, plaintext2)
	require.Equal(t, plaintext2, DecryptOrPlaintext(ciphertext))

	// invalid base64 string
	plaintext2, err = Decrypt("invalid-base64")
	require.True(t, terror.ErrEncCipherTextBase64Decode.Equal(err))
	require.Equal(t, "", plaintext2)
	require.Equal(t, "invalid-base64", DecryptOrPlaintext("invalid-base64"))

	// invalid ciphertext
	plaintext2, err = Decrypt(base64.StdEncoding.EncodeToString([]byte("invalid-plaintext")))
	require.Regexp(t, ".*can not decrypt password.*", err)
	require.Equal(t, "", plaintext2)

	require.Equal(t, "invalid-plaintext", DecryptOrPlaintext("invalid-plaintext"))
}
