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

package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseResource(t *testing.T) {
	tp, suffix, err := ParseResourceID("/local/my-local-resource/a/b/c")
	require.NoError(t, err)
	require.Equal(t, ResourceTypeLocalFile, tp)
	rawResName, err := DecodeResourceName(suffix)
	require.NoError(t, err)
	require.Equal(t, "my-local-resource/a/b/c", rawResName)
	require.Equal(t, "/local/my-local-resource/a/b/c", BuildResourceID(tp, suffix))

	tp, suffix, err = ParseResourceID("/s3/my-local-resource/a/b/c")
	require.NoError(t, err)
	require.Equal(t, ResourceTypeS3, tp)
	rawResName, err = DecodeResourceName(suffix)
	require.NoError(t, err)
	require.Equal(t, "my-local-resource/a/b/c", rawResName)
	require.Equal(t, "/s3/my-local-resource/a/b/c", BuildResourceID(tp, suffix))

	tp, suffix, err = ParseResourceID("/gs/my-local-resource/a/b/c")
	require.NoError(t, err)
	require.Equal(t, ResourceTypeGCS, tp)
	rawResName, err = DecodeResourceName(suffix)
	require.NoError(t, err)
	require.Equal(t, "my-local-resource/a/b/c", rawResName)
	require.Equal(t, "/gs/my-local-resource/a/b/c", BuildResourceID(tp, suffix))

	_, _, err = ParseResourceID("/gcs/my-local-resource/a/b/c")
	require.Error(t, err)
	_, _, err = ParseResourceID("/none/my-local-resource/a/b/c")
	require.Error(t, err)
}

func FuzzEncodeResourceName(f *testing.F) {
	testcases := []string{"resource-1", "resource-1/inner", "!resource-1+-%/*inner"}
	for _, tc := range testcases {
		f.Add(tc)
	}
	f.Fuzz(func(t *testing.T, rawResName string) {
		resName := EncodeResourceName(rawResName)
		decodedResName, err := DecodeResourceName(resName)
		require.NoError(t, err)
		require.Equal(t, rawResName, decodedResName)
	})
}
