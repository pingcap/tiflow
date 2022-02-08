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

package exstorage

import (
	"context"
	"errors"
	"os"
	"path"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang/mock/gomock"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/mock"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestIsS3AndAdjustS3Path(t *testing.T) {
	testPaths := []string{
		"",
		"1invalid:",
		"file:///tmp/storage",
		"/tmp/storage",
		"./tmp/storage",
		"./tmp/storage.mysql-replica-01",
		"tmp/storage",
		"s3:///bucket/more/prefix",
		"s3://bucket2/prefix",
		"s3://bucket3/prefix/path?endpoint=https://127.0.0.1:9000&force_path_style=0&SSE=aws:kms&sse-kms-key-id=TestKey&xyz=abc",
		"s3://bucket4/prefix/path?access-key=NXN7IPIOSAAKDEEOLMAF&secret-access-key=nREY/7Dt+PaIbYKrKlEEMMF/ExCiJEX=XMLPUANw",
		"s3://bucket4/prefix/path.mysql-replica-01?access-key=NXN7IPIOSAAKDEEOLMAF&secret-access-key=nREY/7Dt+PaIbYKrKlEEMMF/ExCiJEX=XMLPUANw",
	}

	testIsS3Results := []struct {
		hasErr bool
		res    bool
		errMsg string
	}{
		{false, false, ""},
		{true, false, "parse (.*)1invalid:(.*): first path segment in URL cannot contain colon*"},
		{false, false, ""},
		{false, false, ""},
		{false, false, ""},
		{false, false, ""},
		{false, false, ""},
		{false, true, ""},
		{false, true, ""},
		{false, true, ""},
		{false, true, ""},
		{false, true, ""},
	}

	for i, testPath := range testPaths {
		isS3, err := IsS3Path(testPath)
		if testIsS3Results[i].hasErr {
			require.Error(t, err)
			require.Regexp(t, testIsS3Results[i].errMsg, err.Error())
		} else {
			require.NoError(t, err)
			require.Equal(t, testIsS3Results[i].res, isS3)
		}
	}

	testAjustResults := []struct {
		hasErr bool
		res    string
	}{
		{false, ""},
		{true, "parse (.*)1invalid:(.*): first path segment in URL cannot contain colon*"},
		{false, "file:///tmp/storage.mysql-replica-01"},
		{false, "/tmp/storage.mysql-replica-01"},
		{false, "./tmp/storage.mysql-replica-01"},
		{false, "./tmp/storage.mysql-replica-01"},
		{false, "tmp/storage.mysql-replica-01"},
		{false, "s3:///bucket/more/prefix.mysql-replica-01"},
		{false, "s3://bucket2/prefix.mysql-replica-01"},
		{false, "s3://bucket3/prefix/path.mysql-replica-01?endpoint=https://127.0.0.1:9000&force_path_style=0&SSE=aws:kms&sse-kms-key-id=TestKey&xyz=abc"},
		{false, "s3://bucket4/prefix/path.mysql-replica-01?access-key=NXN7IPIOSAAKDEEOLMAF&secret-access-key=nREY/7Dt+PaIbYKrKlEEMMF/ExCiJEX=XMLPUANw"},
		{false, "s3://bucket4/prefix/path.mysql-replica-01?access-key=NXN7IPIOSAAKDEEOLMAF&secret-access-key=nREY/7Dt+PaIbYKrKlEEMMF/ExCiJEX=XMLPUANw"},
	}

	for i, testPath := range testPaths {
		newDir, err := AdjustPath(testPath, "mysql-replica-01")
		if testAjustResults[i].hasErr {
			require.Error(t, err)
			require.Regexp(t, testAjustResults[i].res, err.Error())
		} else {
			require.NoError(t, err)
			require.Equal(t, testAjustResults[i].res, newDir)
		}
	}
}

type s3Suite struct {
	controller *gomock.Controller
	s3         *mock.MockS3API
	storage    *storage.S3Storage
}

func createS3Suite(c gomock.TestReporter) (s *s3Suite, clean func()) {
	s = new(s3Suite)
	s.controller = gomock.NewController(c)
	s.s3 = mock.NewMockS3API(s.controller)
	s.storage = storage.NewS3StorageForTest(
		s.s3,
		&backuppb.S3{
			Region:       "us-west-2",
			Bucket:       "bucket",
			Prefix:       "prefix/",
			Acl:          "acl",
			Sse:          "sse",
			StorageClass: "sc",
		},
	)

	clean = func() {
		s.controller.Finish()
	}

	return
}

func TestCollectDirFilesAndRemove(t *testing.T) {
	fileNames := []string{"schema.sql", "table.sql"}

	// test local
	localDir := t.TempDir()
	defer os.RemoveAll(localDir)
	for _, fileName := range fileNames {
		f, err := os.Create(path.Join(localDir, fileName))
		require.NoError(t, err)
		err = f.Close()
		require.NoError(t, err)
	}
	localRes, err := CollectDirFiles(context.Background(), localDir, nil)
	require.NoError(t, err)
	for _, fileName := range fileNames {
		_, ok := localRes[fileName]
		require.True(t, ok)
	}

	// current dir
	pwd, err := os.Getwd()
	require.NoError(t, err)
	tempDir, err := os.MkdirTemp(pwd, "TestCollectDirFiles")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)
	for _, fileName := range fileNames {
		f, err1 := os.Create(path.Join(tempDir, fileName))
		require.NoError(t, err1)
		err1 = f.Close()
		require.NoError(t, err1)
	}
	localRes, err = CollectDirFiles(context.Background(), "./"+path.Base(tempDir), nil)
	require.NoError(t, err)
	for _, fileName := range fileNames {
		_, ok := localRes[fileName]
		require.True(t, ok)
	}

	// test s3
	s, clean := createS3Suite(t)
	defer clean()
	ctx := aws.BackgroundContext()

	objects := make([]*s3.Object, 0, len(fileNames))
	for _, fileName := range fileNames {
		object := &s3.Object{
			Key:  aws.String(path.Join("prefix", fileName)),
			Size: aws.Int64(100),
		}
		objects = append(objects, object)
	}

	s.s3.EXPECT().
		ListObjectsWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsInput, opt ...request.Option) (*s3.ListObjectsOutput, error) {
			require.Equal(t, "bucket", aws.StringValue(input.Bucket))
			require.Equal(t, "prefix/", aws.StringValue(input.Prefix))
			require.Equal(t, "", aws.StringValue(input.Marker))
			require.Equal(t, int64(1), aws.Int64Value(input.MaxKeys))
			require.Equal(t, "", aws.StringValue(input.Delimiter))
			return &s3.ListObjectsOutput{
				IsTruncated: aws.Bool(false),
				Contents:    objects,
			}, nil
		})

	localRes, err = CollectDirFiles(context.Background(), "", s.storage)
	require.NoError(t, err)
	for _, fileName := range fileNames {
		_, ok := localRes[fileName]
		require.True(t, ok)
	}
}

func TestRemoveAll(t *testing.T) {
	fileNames := []string{"schema.sql", "table.sql"}

	// test local
	localDir := t.TempDir()
	defer os.RemoveAll(localDir)
	for _, fileName := range fileNames {
		f, err := os.Create(path.Join(localDir, fileName))
		require.NoError(t, err)
		err = f.Close()
		require.NoError(t, err)
	}
	err := RemoveAll(context.Background(), localDir, nil)
	require.NoError(t, err)
	_, err = os.Stat(localDir)
	require.True(t, os.IsNotExist(err))

	// test s3
	s, clean := createS3Suite(t)
	defer clean()
	ctx := aws.BackgroundContext()

	objects := make([]*s3.Object, 0, len(fileNames))
	for _, fileName := range fileNames {
		object := &s3.Object{
			Key:  aws.String(path.Join("prefix", fileName)),
			Size: aws.Int64(100),
		}
		objects = append(objects, object)
	}

	firstCall := s.s3.EXPECT().
		ListObjectsWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.ListObjectsInput, opt ...request.Option) (*s3.ListObjectsOutput, error) {
			require.Equal(t, "bucket", aws.StringValue(input.Bucket))
			require.Equal(t, "prefix/", aws.StringValue(input.Prefix))
			require.Equal(t, "", aws.StringValue(input.Marker))
			require.Equal(t, int64(1), aws.Int64Value(input.MaxKeys))
			require.Equal(t, "", aws.StringValue(input.Delimiter))
			return &s3.ListObjectsOutput{
				IsTruncated: aws.Bool(false),
				Contents:    objects,
			}, nil
		})
	secondCall := s.s3.EXPECT().
		DeleteObjectWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.DeleteObjectInput, opt ...request.Option) (*s3.DeleteObjectInput, error) {
			require.Equal(t, "bucket", aws.StringValue(input.Bucket))
			require.True(t, aws.StringValue(input.Key) == "prefix/schema.sql" || aws.StringValue(input.Key) == "prefix/table.sql")
			return &s3.DeleteObjectInput{}, nil
		}).After(firstCall)
	thirdCall := s.s3.EXPECT().
		DeleteObjectWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.DeleteObjectInput, opt ...request.Option) (*s3.DeleteObjectInput, error) {
			require.Equal(t, "bucket", aws.StringValue(input.Bucket))
			require.True(t, aws.StringValue(input.Key) == "prefix/schema.sql" || aws.StringValue(input.Key) == "prefix/table.sql")
			return &s3.DeleteObjectInput{}, nil
		}).After(secondCall)
	fourthCall := s.s3.EXPECT().
		DeleteObjectWithContext(ctx, gomock.Any()).
		DoAndReturn(func(_ context.Context, input *s3.DeleteObjectInput, opt ...request.Option) (*s3.DeleteObjectInput, error) {
			require.Equal(t, "bucket", aws.StringValue(input.Bucket))
			require.Equal(t, "prefix/", aws.StringValue(input.Key))
			return &s3.DeleteObjectInput{}, nil
		}).After(thirdCall)

	s.s3.EXPECT().
		HeadObjectWithContext(ctx, gomock.Any()).
		Return(nil, awserr.New(s3.ErrCodeNoSuchKey, "no such key", nil)).After(fourthCall)

	err = RemoveAll(context.Background(), "", s.storage)
	require.NoError(t, err)

	exists, err := s.storage.FileExists(ctx, "")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestIsNotExistError(t *testing.T) {
	// test local
	localDir := t.TempDir()
	defer os.RemoveAll(localDir)
	_, err := os.Open(path.Join(localDir, "test.log"))
	require.Error(t, err)
	res := IsNotExistError(err)
	require.True(t, res)

	// test s3
	s, clean := createS3Suite(t)
	defer clean()
	ctx := aws.BackgroundContext()

	s.s3.EXPECT().
		GetObjectWithContext(ctx, gomock.Any()).
		Return(nil, awserr.New(s3.ErrCodeNoSuchKey, "no such key", nil))

	_, err = s.storage.ReadFile(ctx, "test.log")
	require.Error(t, err)
	res = IsNotExistError(err)
	require.True(t, res)

	// test other local error
	_, err = os.ReadFile(localDir)
	require.Error(t, err)
	res = IsNotExistError(err)
	require.False(t, res)

	// test other s3 error
	s.s3.EXPECT().
		GetObjectWithContext(ctx, gomock.Any()).
		Return(nil, errors.New("just some unrelated error"))

	_, err = s.storage.ReadFile(ctx, "test.log")
	require.Error(t, err)
	res = IsNotExistError(err)
	require.False(t, res)
}
