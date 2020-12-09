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

package util

import (
	"log"
	"strings"
)

type EtcdKey struct {
	keyStr string
}

func NewEtcdKey(key string) EtcdKey {
	return EtcdKey{key}
}

func NewEtcdKeyFromBytes(key []byte) EtcdKey {
	return EtcdKey{string(key)}
}

func (key *EtcdKey) Prefix() EtcdPrefix {
	if index := strings.LastIndexByte(key.keyStr, '/'); index >= 0 {
		return EtcdPrefix{key.keyStr[:index]}
	} else {
		return EtcdPrefix{""}
	}
}

func (key *EtcdKey) String() string {
	return key.keyStr
}

func (key *EtcdKey) Bytes() []byte {
	return []byte(key.keyStr)
}

func (key *EtcdKey) Head() EtcdPrefix {
	if len(key.keyStr) == 0 {
		log.Panic("Empty EtcdKey")
	}
	if index := strings.IndexByte(key.keyStr[1:], '/'); index >= 0 {
		return EtcdPrefix{key.keyStr[:index+1]}
	} else {
		return EtcdPrefix{""}
	}
}

func (key *EtcdKey) Tail() EtcdRelKey {
	if index := strings.IndexByte(key.keyStr[1:], '/'); index >= 0 {
		return EtcdRelKey{EtcdKey{key.keyStr[index+1:]}}
	} else {
		return EtcdRelKey{EtcdKey{key.keyStr}}
	}
}

func (key *EtcdKey) RemovePrefix(prefix *EtcdPrefix) EtcdRelKey {
	return EtcdRelKey{EtcdKey{strings.TrimPrefix(key.keyStr, prefix.prefixStr)}}
}

type EtcdRelKey struct {
	inner EtcdKey
}

func NewEtcdRelKey(key string) EtcdRelKey {
	return EtcdRelKey{NewEtcdKey(key)}
}

func NewEtcdRelKeyFromBytes(key []byte) EtcdRelKey {
	return EtcdRelKey{NewEtcdKeyFromBytes(key)}
}

func (rkey *EtcdRelKey) String() string{
	return rkey.inner.String()
}

func (rkey *EtcdRelKey) Bytes() []byte {
	return rkey.inner.Bytes()
}

func (rkey *EtcdRelKey) Head() EtcdRelPrefix {
	return EtcdRelPrefix{rkey.inner.Head()}
}

func (rkey *EtcdRelKey) Tail() EtcdRelKey {
	return rkey.inner.Tail()
}

func (key *EtcdRelKey) RemovePrefix(prefix *EtcdRelPrefix) EtcdRelKey {
	return EtcdRelKey{EtcdKey{strings.TrimPrefix(key.inner.keyStr, prefix.prefixStr)}}
}

type EtcdPrefix struct {
	prefixStr string
}

func (prefix *EtcdPrefix) String() string {
	return prefix.prefixStr
}

func (prefix *EtcdPrefix) Bytes() []byte {
	return []byte(prefix.prefixStr)
}

func (prefix *EtcdPrefix) Head() EtcdPrefix {
	if len(prefix.prefixStr) == 0 {
		log.Panic("Empty EtcdPrefix")
	}
	if index := strings.IndexByte(prefix.prefixStr[1:], '/'); index >= 0 {
		return EtcdPrefix{prefix.prefixStr[:index+1]}
	} else {
		return EtcdPrefix{""}
	}
}

func (prefix *EtcdPrefix) Tail() EtcdRelPrefix {
	if len(prefix.prefixStr) == 0 {
		log.Panic("Empty EtcdPrefix")
	}
	if index := strings.IndexByte(prefix.prefixStr[1:], '/'); index >= 0 {
		return EtcdRelPrefix{EtcdPrefix{prefix.prefixStr[index+1:]}}
	} else {
		return EtcdRelPrefix{EtcdPrefix{prefix.prefixStr}}
	}
}

func (prefix *EtcdPrefix) FullKey(key *EtcdRelKey) EtcdKey {
	return EtcdKey{prefix.prefixStr + key.inner.keyStr}
}

type EtcdRelPrefix struct {
	EtcdPrefix
}

// NormalizePrefix adds a slash to the beginning of `prefix` if none is present,
// and removes a trailing slash, if one is present.
func NormalizePrefix(prefix string) EtcdPrefix {
	ret := prefix
	if !strings.HasPrefix(prefix, "/") {
		ret = "/" + prefix
	}
	return EtcdPrefix {strings.TrimSuffix(ret, "/")}
}
