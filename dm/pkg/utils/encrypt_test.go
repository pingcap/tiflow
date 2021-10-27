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

	. "github.com/pingcap/check"

	"github.com/pingcap/dm/pkg/terror"
)

var _ = Suite(&testEncryptSuite{})

type testEncryptSuite struct{}

func (t *testEncryptSuite) TestEncrypt(c *C) {
	plaintext := "abc@123"
	ciphertext, err := Encrypt(plaintext)
	c.Assert(err, IsNil)

	plaintext2, err := Decrypt(ciphertext)
	c.Assert(err, IsNil)
	c.Assert(plaintext2, Equals, plaintext)
	c.Assert(DecryptOrPlaintext(ciphertext), Equals, plaintext2)

	// invalid base64 string
	plaintext2, err = Decrypt("invalid-base64")
	c.Assert(terror.ErrEncCipherTextBase64Decode.Equal(err), IsTrue)
	c.Assert(plaintext2, Equals, "")
	c.Assert(DecryptOrPlaintext("invalid-base64"), Equals, "invalid-base64")

	// invalid ciphertext
	plaintext2, err = Decrypt(base64.StdEncoding.EncodeToString([]byte("invalid-plaintext")))
	c.Assert(err, ErrorMatches, ".*can not decrypt password.*")
	c.Assert(plaintext2, Equals, "")
	c.Assert(DecryptOrPlaintext("invalid-plaintext"), Equals, "invalid-plaintext")
}
