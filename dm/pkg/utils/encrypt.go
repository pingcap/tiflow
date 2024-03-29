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

package utils

import (
	"encoding/base64"

	"github.com/pingcap/tiflow/dm/pkg/encrypt"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

// Encrypt tries to encrypt plaintext to base64 encoded ciphertext.
func Encrypt(plaintext string) (string, error) {
	ciphertext, err := encrypt.Encrypt([]byte(plaintext))
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// EncryptOrPlaintext tries to encrypt plaintext to base64 encoded ciphertext or return plaintext.
// dm-master might not set customized key, so we should handle the error and return plaintext directly.
func EncryptOrPlaintext(plaintext string) string {
	ciphertext, err := Encrypt(plaintext)
	if err != nil {
		return plaintext
	}
	return ciphertext
}

// Decrypt tries to decrypt base64 encoded ciphertext to plaintext.
func Decrypt(ciphertextB64 string) (string, error) {
	ciphertext, err := base64.StdEncoding.DecodeString(ciphertextB64)
	if err != nil {
		return "", terror.ErrEncCipherTextBase64Decode.Delegate(err, ciphertextB64)
	}

	plaintext, err := encrypt.Decrypt(ciphertext)
	if err != nil {
		return "", terror.Annotatef(err, "can not decrypt password %s", ciphertextB64)
	}
	return string(plaintext), nil
}

// DecryptOrPlaintext tries to decrypt base64 encoded ciphertext to plaintext or return plaintext.
// when a customized key is provided, we support both plaintext and ciphertext as password,
// if not provided, only plaintext is supported.
func DecryptOrPlaintext(ciphertextB64 string) string {
	plaintext, err := Decrypt(ciphertextB64)
	if err != nil {
		return ciphertextB64
	}
	return plaintext
}
