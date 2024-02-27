// Copyright 2019 PingCAP, Inc.
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

package encrypt

import (
	"crypto/aes"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func removeChar(input []byte, c byte) []byte {
	i := 0
	for _, x := range input {
		if x != c {
			input[i] = x
			i++
		}
	}
	return input[:i]
}

func TestCipher(t *testing.T) {
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)

	t.Cleanup(func() {
		InitCipher(nil)
	})
	InitCipher(nil)
	require.IsType(t, &notInitializedCipher{}, defaultCipher)

	InitCipher(key)
	require.IsType(t, &aesCipher{}, defaultCipher)

	plaintext := []byte("a plain text")

	// encrypt
	ciphertext, err := Encrypt(plaintext)
	require.NoError(t, err)

	// decrypt
	plaintext2, err := Decrypt(ciphertext)
	require.NoError(t, err)
	require.Equal(t, plaintext, plaintext2)

	// invalid length
	_, err = Decrypt(ciphertext[:len(ciphertext)-len(plaintext)-1])
	require.Error(t, err)

	// invalid content
	_, err = Decrypt(removeChar(ciphertext, ivSep[0]))
	require.Error(t, err)

	// a special case, we construct a ciphertext that can be decrypted but the
	// plaintext is not what we want. This is because currently encrypt mechanism
	// doesn't keep enough information to decide whether the new ciphertext is valid
	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	blockSize := block.BlockSize()
	require.Greater(t, len(ciphertext), blockSize+2)
	plaintext3, err := Decrypt(append(ciphertext[1:blockSize+1], append([]byte{ivSep[0]}, ciphertext[blockSize+2:]...)...))
	require.NoError(t, err)
	require.NotEqual(t, plaintext, plaintext3)
}
