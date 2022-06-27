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

package adapter

import (
	"encoding/hex"
	"path"
	"strings"

	"github.com/pingcap/tiflow/pkg/errors"
)

// Defines all key adapters
var (
	MasterCampaignKey KeyAdapter = keyHexEncoderDecoder("/data-flow/master/leader/")
	// TODO: investigate whether we can merge MasterInfoKey and MasterMetaKey into one key
	MasterInfoKey      KeyAdapter = keyHexEncoderDecoder("/data-flow/master/info/")
	MasterMetaKey      KeyAdapter = keyHexEncoderDecoder("/data-flow/master/meta/")
	NodeInfoKeyAdapter KeyAdapter = keyHexEncoderDecoder("/data-flow/node/info/")
	JobKeyAdapter      KeyAdapter = keyHexEncoderDecoder("/data-flow/job/")
	TaskKeyAdapter     KeyAdapter = keyHexEncoderDecoder("/data-flow/task/")
	WorkerKeyAdapter   KeyAdapter = keyHexEncoderDecoder("/data-flow/worker/")
	UserKeyAdapter     KeyAdapter = keyHexEncoderDecoder("/data-flow/user/")

	ResourceKeyAdapter KeyAdapter = keyHexEncoderDecoder("/data-flow/resources/")

	// TODO: discuss the key prefix
	DMJobKeyAdapter KeyAdapter = keyHexEncoderDecoder("/data-flow/dm/job/")
)

// KeyAdapter is used to construct etcd like key
type KeyAdapter interface {
	Encode(keys ...string) string
	Decode(key string) ([]string, error)
	Path() string
	Curry(keys ...string) KeyAdapter
}

type keyHexEncoderDecoder string

func (s keyHexEncoderDecoder) Encode(keys ...string) string {
	hexKeys := []string{string(s)}
	for _, key := range keys {
		hexKeys = append(hexKeys, hex.EncodeToString([]byte(key)))
	}
	ret := path.Join(hexKeys...)
	//if len(keys) < keyAdapterKeysLen(s) {
	//	ret += "/"
	//}
	return ret
}

func (s keyHexEncoderDecoder) Decode(key string) ([]string, error) {
	if key[len(key)-1] == '/' {
		key = key[:len(key)-1]
	}
	v := strings.Split(strings.TrimPrefix(key, string(s)), "/")
	//if l := keyAdapterKeysLen(s); l != len(v) {
	//	return nil, terror.ErrDecodeEtcdKeyFail.Generate(fmt.Sprintf("decoder is %s, the key is %s", string(s), key))
	//}
	for i, k := range v {
		dec, err := hex.DecodeString(k)
		if err != nil {
			return nil, errors.WrapError(errors.ErrDecodeEtcdKeyFail, err, k)
		}
		v[i] = string(dec)
	}
	return v, nil
}

func (s keyHexEncoderDecoder) Path() string {
	return string(s)
}

func (s keyHexEncoderDecoder) Curry(keys ...string) KeyAdapter {
	prefix := s.Encode(keys...)
	if prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}
	return keyHexEncoderDecoder(prefix)
}
