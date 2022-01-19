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
	"strings"

	"github.com/pingcap/log"
)

// EtcdKey represents a complete key in Etcd.
type EtcdKey struct {
	keyStr string
}

// NewEtcdKey creates an EtcdKey for the given string.
func NewEtcdKey(key string) EtcdKey {
	return EtcdKey{key}
}

// NewEtcdKeyFromBytes creates an EtcdKey for the given bytes.
func NewEtcdKeyFromBytes(key []byte) EtcdKey {
	return EtcdKey{string(key)}
}

// String returns the string representation of the key.
func (key EtcdKey) String() string {
	return key.keyStr
}

// Bytes returns the bytes representation of the key.
func (key EtcdKey) Bytes() []byte {
	return []byte(key.keyStr)
}

// Head returns a EtcdRelPrefix that is the first segment of the key.
// For example, if key.String() == "/a/b/c", then key.Head() == "/a".
func (key EtcdKey) Head() EtcdPrefix {
	if len(key.keyStr) == 0 {
		log.Panic("Empty EtcdKey")
	}
	if index := strings.IndexByte(key.keyStr[1:], '/'); index >= 0 {
		return EtcdPrefix{key.keyStr[:index+1]}
	}
	return EtcdPrefix{""}
}

// Tail returns an EtcdRelKey that is the key without the first segment.
// For example, if key.String() == "/a/b/c", then key.Tail() == "/b/c".
func (key EtcdKey) Tail() EtcdRelKey {
	if len(key.keyStr) == 0 {
		log.Panic("Empty EtcdKey")
	}
	if index := strings.IndexByte(key.keyStr[1:], '/'); index >= 0 {
		return EtcdRelKey{EtcdKey{key.keyStr[index+1:]}}
	}
	return EtcdRelKey{EtcdKey{key.keyStr}}
}

// RemovePrefix removes a prefix from the key.
// It is a wrapper for strings.TrimPrefix.
func (key EtcdKey) RemovePrefix(prefix *EtcdPrefix) EtcdRelKey {
	return EtcdRelKey{EtcdKey{strings.TrimPrefix(key.keyStr, prefix.prefixStr)}}
}

// AsRelKey casts the EtcdKey to an EtcdRelKey.
func (key EtcdKey) AsRelKey() EtcdRelKey {
	return NewEtcdRelKey(key.keyStr)
}

// EtcdRelKey represents a string that might be used as a suffix of a valid Etcd key.
type EtcdRelKey struct {
	inner EtcdKey
}

// NewEtcdRelKey creates an EtcdRelKey for the given string.
func NewEtcdRelKey(key string) EtcdRelKey {
	return EtcdRelKey{NewEtcdKey(key)}
}

// NewEtcdRelKeyFromBytes creates an EtcdRelKey for the given bytes.
func NewEtcdRelKeyFromBytes(key []byte) EtcdRelKey {
	return EtcdRelKey{NewEtcdKeyFromBytes(key)}
}

// AsPrefix casts EtcdRelKey into EtcdRelPrefix.
func (rkey *EtcdRelKey) AsPrefix() EtcdRelPrefix {
	return EtcdRelPrefix{EtcdPrefix{rkey.String()}}
}

// String returns the string representation of the key.
func (rkey EtcdRelKey) String() string {
	return rkey.inner.String()
}

// Bytes returns the bytes representation of the key.
func (rkey EtcdRelKey) Bytes() []byte {
	return rkey.inner.Bytes()
}

// Head returns an EtcdRelPrefix that is the first segment of the key.
// For example, if key.String() == "/a/b/c", then key.Head() == "/a".
func (rkey EtcdRelKey) Head() EtcdRelPrefix {
	return EtcdRelPrefix{rkey.inner.Head()}
}

// Tail returns an EtcdRelKey that is the key without the first segment.
// For example, if key.String() == "/a/b/c", then key.Tail() == "/b/c".
func (rkey EtcdRelKey) Tail() EtcdRelKey {
	return rkey.inner.Tail()
}

// RemovePrefix removes a prefix from the key.
// It is a wrapper for strings.TrimPrefix.
func (rkey EtcdRelKey) RemovePrefix(prefix *EtcdRelPrefix) EtcdRelKey {
	return EtcdRelKey{EtcdKey{strings.TrimPrefix(rkey.inner.keyStr, prefix.prefixStr)}}
}

// EtcdPrefix represents a string that might be the prefix of a valid key in Etcd.
type EtcdPrefix struct {
	prefixStr string
}

// String returns the string representation of the EtcdPrefix.
func (prefix EtcdPrefix) String() string {
	return prefix.prefixStr
}

// Bytes returns the bytes representation of the EtcdPrefix.
func (prefix EtcdPrefix) Bytes() []byte {
	return []byte(prefix.prefixStr)
}

// Head returns a EtcdPrefix that is the first segment of the given prefix.
func (prefix EtcdPrefix) Head() EtcdPrefix {
	if len(prefix.prefixStr) == 0 {
		log.Panic("Empty EtcdPrefix")
	}
	if index := strings.IndexByte(prefix.prefixStr[1:], '/'); index >= 0 {
		return EtcdPrefix{prefix.prefixStr[:index+1]}
	}
	return EtcdPrefix{""}
}

// Tail returns a EtcdRelPrefix that is the given prefix with its first segment removed.
func (prefix EtcdPrefix) Tail() EtcdRelPrefix {
	if len(prefix.prefixStr) == 0 {
		log.Panic("Empty EtcdPrefix")
	}
	if index := strings.IndexByte(prefix.prefixStr[1:], '/'); index >= 0 {
		return EtcdRelPrefix{EtcdPrefix{prefix.prefixStr[index+1:]}}
	}
	return EtcdRelPrefix{EtcdPrefix{prefix.prefixStr}}
}

// FullKey transforms an EtcdRelKey to an EtcdKey by adding the prefix to it.
func (prefix EtcdPrefix) FullKey(key EtcdRelKey) EtcdKey {
	return EtcdKey{prefix.prefixStr + key.inner.keyStr}
}

// EtcdRelPrefix represents a prefix to a meaningful suffix of a valid EtcdKey.
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
	return NewEtcdPrefix(strings.TrimSuffix(ret, "/"))
}

// NewEtcdPrefix creates an EtcdPrefix from the given string.
// For a safer version, use NormalizePrefix.
func NewEtcdPrefix(prefix string) EtcdPrefix {
	return EtcdPrefix{prefix}
}

// NewEtcdRelPrefix creates an EtcdRelPrefix from the given string.
func NewEtcdRelPrefix(prefix string) EtcdRelPrefix {
	return EtcdRelPrefix{NewEtcdPrefix(prefix)}
}

// NewEtcdPrefixFromBytes creates an EtcdPrefix from the given bytes.
// For a safer version, use NormalizePrefix.
func NewEtcdPrefixFromBytes(prefix []byte) EtcdPrefix {
	return NewEtcdPrefix(string(prefix))
}

// NewEtcdRelPrefixFromBytes creates an EtcdRelPrefix from the given bytes.
func NewEtcdRelPrefixFromBytes(prefix []byte) EtcdRelPrefix {
	return NewEtcdRelPrefix(string(prefix))
}
