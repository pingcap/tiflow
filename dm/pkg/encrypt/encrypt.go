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
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/pkg/errors"
)

var ivSep = []byte("@") // ciphertext format: iv + ivSep + encrypted-plaintext

// Cipher is the interface for encrypt/decrypt.
type Cipher interface {
	Encrypt(plaintext []byte) ([]byte, error)
	Decrypt(ciphertext []byte) ([]byte, error)
}

// defaultCipher is the default cipher, it should be initialized using
// InitCipher before any call to Encrypt and Decrypt.
// we keep it as a global variable to avoid change existing call site.
var defaultCipher Cipher = &notInitializedCipher{}

// InitCipher initializes the defaultCipher.
func InitCipher(key []byte) {
	if len(key) == 0 {
		log.L().Warn("empty secret key, password in config file will be in plain text")
		defaultCipher = &notInitializedCipher{}
		return
	}
	log.L().Info("password in config file will be encrypted")
	defaultCipher = &aesCipher{secretKey: key}
}

// IsInitialized returns whether the defaultCipher is initialized or not.
func IsInitialized() bool {
	_, ok := defaultCipher.(*aesCipher)
	return ok
}

// DM will guess whether the password is encrypted or not.
// to compatible with this behavior when InitCipher is not called,
// we use a notInitializedCipher to always return error.
type notInitializedCipher struct{}

func (n *notInitializedCipher) Encrypt([]byte) ([]byte, error) {
	return nil, errors.New("secret key is not initialized")
}

func (n *notInitializedCipher) Decrypt([]byte) ([]byte, error) {
	return nil, errors.New("secret key is not initialized")
}

type aesCipher struct {
	secretKey []byte
}

func (c *aesCipher) Encrypt(plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(c.secretKey)
	if err != nil {
		return nil, terror.ErrEncryptGenCipher.Delegate(err)
	}

	iv, err := genIV(block.BlockSize())
	if err != nil {
		return nil, err
	}

	ciphertext := make([]byte, 0, len(iv)+len(ivSep)+len(plaintext))
	ciphertext = append(ciphertext, iv...)
	ciphertext = append(ciphertext, ivSep...)
	ciphertext = append(ciphertext, plaintext...) // will be overwrite by XORKeyStream

	stream := cipher.NewCFBEncrypter(block, iv)
	stream.XORKeyStream(ciphertext[len(iv)+len(ivSep):], plaintext)

	return ciphertext, nil
}

func (c *aesCipher) Decrypt(ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(c.secretKey)
	if err != nil {
		return nil, err
	}

	if len(ciphertext) < block.BlockSize()+len(ivSep) {
		return nil, terror.ErrCiphertextLenNotValid.Generate(block.BlockSize()+len(ivSep), len(ciphertext))
	}

	if !bytes.Equal(ciphertext[block.BlockSize():block.BlockSize()+len(ivSep)], ivSep) {
		return nil, terror.ErrCiphertextContextNotValid.Generate()
	}

	iv := ciphertext[:block.BlockSize()]
	ciphertext = ciphertext[block.BlockSize()+len(ivSep):]
	plaintext := make([]byte, len(ciphertext))

	stream := cipher.NewCFBDecrypter(block, iv)
	stream.XORKeyStream(plaintext, ciphertext)

	return plaintext, nil
}

func genIV(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	return b, terror.ErrEncryptGenIV.Delegate(err)
}

// Encrypt encrypts plaintext to ciphertext.
func Encrypt(plaintext []byte) ([]byte, error) {
	return defaultCipher.Encrypt(plaintext)
}

// Decrypt decrypts ciphertext to plaintext.
func Decrypt(ciphertext []byte) ([]byte, error) {
	return defaultCipher.Decrypt(ciphertext)
}
