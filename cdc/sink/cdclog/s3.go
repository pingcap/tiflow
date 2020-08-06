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

package cdclog

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/ticdc/cdc/model"
)

const (
	s3EndpointOption     = "s3.endpoint"
	s3RegionOption       = "s3.region"
	s3StorageClassOption = "s3.storage-class"
	s3SseOption          = "s3.sse"
	s3SseKmsKeyIDOption  = "s3.sse-kms-key-id"
	s3ACLOption          = "s3.acl"
	s3ProviderOption     = "s3.provider"
	// number of retries to make of operations
	maxRetries = 3

	maxBufferKeys = 1000

	maxPartFlushSize = 5 << 20 // The minimal multipart upload size is 5Mb.

	maxCompletePartSize = 100 << 20

	maxDDLFlushSize = 10 << 20

	defaultBufferChanSize = 1280

	defaultFlushRowChangedEventDuration = 5 * time.Second // TODO make it as a config
)

// S3Options contains options for s3 storage
type S3Options struct {
	Endpoint              string `json:"endpoint" toml:"endpoint"`
	Region                string `json:"region" toml:"region"`
	StorageClass          string `json:"storage-class" toml:"storage-class"`
	Sse                   string `json:"sse" toml:"sse"`
	SseKmsKeyID           string `json:"sse-kms-key-id" toml:"sse-kms-key-id"`
	ACL                   string `json:"acl" toml:"acl"`
	AccessKey             string `json:"access-key" toml:"access-key"`
	SecretAccessKey       string `json:"secret-access-key" toml:"secret-access-key"`
	Provider              string `json:"provider" toml:"provider"`
	ForcePathStyle        bool   `json:"force-path-style" toml:"force-path-style"`
	UseAccelerateEndpoint bool   `json:"use-accelerate-endpoint" toml:"use-accelerate-endpoint"`
}

func (options *S3Options) extractFromQuery(sinkURI *url.URL) *S3Options {
	options.Endpoint = sinkURI.Query().Get(s3EndpointOption)
	options.Region = sinkURI.Query().Get(s3RegionOption)
	options.Sse = sinkURI.Query().Get(s3SseOption)
	options.SseKmsKeyID = sinkURI.Query().Get(s3SseKmsKeyIDOption)
	options.ACL = sinkURI.Query().Get(s3ACLOption)
	options.StorageClass = sinkURI.Query().Get(s3StorageClassOption)
	options.ForcePathStyle = true
	options.Provider = sinkURI.Query().Get(s3ProviderOption)
	// TODO remove this access key, use env variable instead
	options.SecretAccessKey = sinkURI.Query().Get("secret-access-key")
	options.AccessKey = sinkURI.Query().Get("access-key")
	return options
}

func (options *S3Options) adjust() error {
	if len(options.Region) == 0 {
		options.Region = "us-east-1"
	}
	if len(options.Endpoint) != 0 {
		u, err := url.Parse(options.Endpoint)
		if err != nil {
			return err
		}
		if len(u.Scheme) == 0 {
			return errors.New("scheme not found in endpoint")
		}
		if len(u.Host) == 0 {
			return errors.New("host not found in endpoint")
		}
	}
	// In some cases, we need to set ForcePathStyle to false.
	// Refer to: https://rclone.org/s3/#s3-force-path-style
	if options.Provider == "alibaba" || options.Provider == "netease" ||
		options.UseAccelerateEndpoint {
		options.ForcePathStyle = false
	}
	if len(options.AccessKey) == 0 && len(options.SecretAccessKey) != 0 {
		return errors.New("access_key not found")
	}
	if len(options.AccessKey) != 0 && len(options.SecretAccessKey) == 0 {
		return errors.New("secret_access_key not found")
	}

	return nil
}

func (options *S3Options) apply() *aws.Config {
	awsConfig := aws.NewConfig().
		WithMaxRetries(maxRetries).
		WithS3ForcePathStyle(options.ForcePathStyle).
		WithRegion(options.Region)

	if len(options.Endpoint) != 0 {
		awsConfig.WithEndpoint(options.Endpoint)
	}
	var cred *credentials.Credentials
	if len(options.AccessKey) != 0 && len(options.SecretAccessKey) != 0 {
		cred = credentials.NewStaticCredentials(options.AccessKey, options.SecretAccessKey, "")
	}
	if cred != nil {
		awsConfig.WithCredentials(cred)
	}
	return awsConfig
}

type tableBuffer struct {
	// for log
	lock       sync.RWMutex
	tableID    int64
	dataCh     chan *model.RowChangedEvent
	sendKeys   int64
	sendEvents int64

	uploadParts struct {
		originPartUploadResponse *s3.CreateMultipartUploadOutput
		byteSize                 int64
		completeParts            []*s3.CompletedPart
	}
}

func (tb *tableBuffer) Flush(ctx context.Context, s *s3Sink) error {
	tb.lock.Lock()
	sendEvents := tb.sendEvents
	hashPart := tb.uploadParts
	tb.lock.Unlock()

	if sendEvents == 0 && len(hashPart.completeParts) == 0 {
		log.Info("nothing to flush", zap.Int64("tableID", tb.tableID))
		return nil
	}

	var newFileName string
	flushedKeys := int64(0)
	rowDatas := make([]byte, 0)
	for event := int64(0); event < sendEvents; event++ {
		row := <-tb.dataCh
		flushedKeys += int64(len(row.Keys))
		if event == sendEvents-1 {
			// if last event, we record ts as new rotate file name
			newFileName = makeTableFileObject(row.Table.TableID, row.CommitTs)
		}

		data, err := row.ToProtoBuf().Marshal()
		if err != nil {
			return err
		}
		rowDatas = append(rowDatas, data...)
		rowDatas = append(rowDatas, '\n')
	}

	log.Debug("[FlushRowChangedEvents[Debug]] flush table buffer",
		zap.Int64("table", tb.tableID),
		zap.Int64("event size", sendEvents),
		zap.Int("row data size", len(rowDatas)),
		zap.Int("upload num", len(hashPart.completeParts)),
		zap.Int64("upload byte size", hashPart.byteSize),
		// zap.ByteString("rowDatas", rowDatas),
	)

	if len(rowDatas) > 0 {
		if len(rowDatas) > maxPartFlushSize || len(hashPart.completeParts) > 0 {
			// S3 multi-upload need every chunk(except the last one) is greater than 5Mb
			// so, if this batch data size is greater than 5Mb or it has uploadPart already
			// we will use multi-upload this batch data
			if hashPart.originPartUploadResponse == nil {
				resp, err := s.createMultipartUpload(newFileName)
				if err != nil {
					return err
				}
				hashPart.originPartUploadResponse = resp
			}

			completePart, err := s.uploadPart(hashPart.originPartUploadResponse, rowDatas, len(tb.uploadParts.completeParts))
			if err != nil {
				return err
			}

			hashPart.byteSize += int64(len(rowDatas))
			hashPart.completeParts = append(hashPart.completeParts, completePart)

			if hashPart.byteSize > maxCompletePartSize || len(rowDatas) <= maxPartFlushSize {
				// we need do complete when total upload size is greater than 100Mb
				// or this part data is less than 5Mb to avoid meet EntityTooSmall error
				log.Info("[FlushRowChangedEvents] complete file",
					zap.Int64("tableID", tb.tableID),
					zap.Stringer("resp", hashPart.originPartUploadResponse))
				_, err := s.completeMultipartUpload(hashPart.originPartUploadResponse, hashPart.completeParts)
				if err != nil {
					return err
				}
				hashPart.byteSize = 0
				hashPart.completeParts = hashPart.completeParts[:0]
				hashPart.originPartUploadResponse = nil
			}
		} else {
			// generate normal file because S3 multi-upload need every part at least 5Mb.
			log.Info("[FlushRowChangedEvents] normal upload file",
				zap.Int64("tableID", tb.tableID))
			err := s.putObjectWithKey(ctx, s.prefix+newFileName, rowDatas)
			if err != nil {
				return err
			}
		}
	}

	// re-set counter
	tb.lock.Lock()
	tb.uploadParts = hashPart
	tb.sendEvents -= sendEvents
	tb.sendKeys -= flushedKeys
	tb.lock.Unlock()

	return nil
}

func newTableBuffer(tableID int64) *tableBuffer {
	return &tableBuffer{
		tableID:    tableID,
		dataCh:     make(chan *model.RowChangedEvent, defaultBufferChanSize),
		sendKeys:   0,
		sendEvents: 0,
		uploadParts: struct {
			originPartUploadResponse *s3.CreateMultipartUploadOutput
			byteSize                 int64
			completeParts            []*s3.CompletedPart
		}{originPartUploadResponse: nil,
			byteSize:      0,
			completeParts: make([]*s3.CompletedPart, 0, 128),
		},
	}
}

type s3Sink struct {
	bucket string
	prefix string

	options *S3Options
	client  *s3.S3

	logMeta *logMeta

	hashMap      sync.Map
	tableBuffers []*tableBuffer
	notifyChan   chan struct{}
}

func (s *s3Sink) HeadObject(name string) (*s3.HeadObjectOutput, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(name),
	}
	return s.client.HeadObject(input)
}

func (s *s3Sink) ListObject(name string, maxKeys int64) (*s3.ListObjectsV2Output, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.bucket),
		Prefix:  aws.String(s.prefix + name),
		MaxKeys: aws.Int64(maxKeys),
	}
	return s.client.ListObjectsV2(input)
}

func (s *s3Sink) putObjectWithKey(ctx context.Context, key string, data []byte) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	if len(data) != 0 {
		input.Body = aws.ReadSeekCloser(bytes.NewReader(data))
	}
	if len(s.options.ACL) != 0 {
		input = input.SetACL(s.options.ACL)
	}
	if len(s.options.Sse) != 0 {
		input = input.SetServerSideEncryption(s.options.Sse)
	}
	if len(s.options.SseKmsKeyID) != 0 {
		input = input.SetSSEKMSKeyId(s.options.SseKmsKeyID)
	}
	if len(s.options.StorageClass) != 0 {
		input = input.SetStorageClass(s.options.StorageClass)
	}

	_, err := s.client.PutObjectWithContext(ctx, input)
	if err != nil {
		return err
	}
	hInput := &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	return s.client.WaitUntilObjectExistsWithContext(ctx, hInput)
}

func (s *s3Sink) getObjectWithKey(ctx context.Context, key string) (*s3.GetObjectOutput, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	return s.client.GetObjectWithContext(ctx, input)
}

func (s *s3Sink) createMultipartUpload(name string) (*s3.CreateMultipartUploadOutput, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.prefix + name),
	}
	return s.client.CreateMultipartUpload(input)
}

func (s *s3Sink) completeMultipartUpload(resp *s3.CreateMultipartUploadOutput, completedParts []*s3.CompletedPart) (*s3.CompleteMultipartUploadOutput, error) {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}
	return s.client.CompleteMultipartUpload(completeInput)
}

func (s *s3Sink) uploadPart(resp *s3.CreateMultipartUploadOutput, fileBytes []byte, partNumber int) (*s3.CompletedPart, error) {
	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(fileBytes),
		Bucket:        resp.Bucket,
		Key:           resp.Key,
		PartNumber:    aws.Int64(int64(partNumber)),
		UploadId:      resp.UploadId,
		ContentLength: aws.Int64(int64(len(fileBytes))),
	}

	uploadResult, err := s.client.UploadPart(partInput)
	if err != nil {
		return nil, err
	}
	return &s3.CompletedPart{
		ETag:       uploadResult.ETag,
		PartNumber: aws.Int64(int64(partNumber)),
	}, nil
}

func (s *s3Sink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	shouldFlush := false
	for _, row := range rows {
		// dispatch row event by tableID
		tableID := row.Table.GetTableID()
		var (
			ok   bool
			item interface{}
			hash int
		)
		if item, ok = s.hashMap.Load(tableID); !ok {
			// found new tableID
			s.tableBuffers = append(s.tableBuffers, newTableBuffer(tableID))
			hash = len(s.tableBuffers) - 1
			s.hashMap.Store(tableID, hash)
		} else {
			hash = item.(int)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.tableBuffers[hash].dataCh <- row:
			s.tableBuffers[hash].lock.RLock()
			s.tableBuffers[hash].sendKeys += int64(len(row.Keys))
			if s.tableBuffers[hash].sendKeys > maxBufferKeys {
				// trigger flush when a table has maxBufferKeys
				shouldFlush = true
			}
			s.tableBuffers[hash].sendEvents += 1
			s.tableBuffers[hash].lock.RUnlock()
		}
	}
	if shouldFlush {
		s.notifyChan <- struct{}{}
	}
	return nil
}

func (s *s3Sink) flushTableBuffers(ctx context.Context) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(s.tableBuffers))
	for _, tb := range s.tableBuffers {
		wg.Add(1)
		tbReplica := tb
		// TODO use a fixed worker pool
		go func(ctx context.Context, tb *tableBuffer) {
			defer wg.Done()
			log.Info("[FlushRowChangedEvents] flush specify row changed event",
				zap.Int64("table", tb.tableID),
				zap.Int64("event size", tb.sendEvents))
			errCh <- tb.Flush(ctx, s)
		}(ctx, tbReplica)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *s3Sink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) error {
	// we should flush all events before resolvedTs, there are two kind of flush policy
	// 1. flush row events to a s3 chunk: if the event size is not enough,
	//    TODO: when cdc crashed, we should repair these chunks to a complete file
	// 2. flush row events to a complete s3 file: if the event size is enough
	select {
	case <-s.notifyChan:
		return s.flushTableBuffers(ctx)

	case <-time.After(defaultFlushRowChangedEventDuration):
		// cannot accumulate enough row events in 10 second
		// flush all tables' row events to s3
		return s.flushTableBuffers(ctx)
	}
}

// EmitCheckpointTs update the global resolved ts in log meta
// sleep 5 seconds to avoid update too frequently
func (s *s3Sink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	if s.logMeta == nil {
		log.Debug("[EmitCheckpointTs] generate new logMeta when emit checkpoint ts", zap.Uint64("ts", ts))
		s.logMeta = new(logMeta)
	}
	s.logMeta.GlobalResolvedTS = ts
	data, err := s.logMeta.Marshal()
	if err != nil {
		return errors.Annotate(err, "marshal meta to json failed")
	}
	return s.putObjectWithKey(ctx, s.prefix+logMetaFile, data)
}

// EmitDDLEvent write ddl event to S3 directory, all events split by '\n'
// Because S3 doesn't support append-like write.
// we choose a hack way to read origin file then write in place.
func (s *s3Sink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	data, err := ddl.ToProtoBuf().Marshal()
	if err != nil {
		return err
	}
	listing, err := s.ListObject(ddlEventsDir, 1)
	if err != nil {
		return err
	}
	var name string
	size := int64(0)
	for _, content := range listing.Contents {
		name = *content.Key
		size = *content.Size
		log.Debug("[EmitDDLEvent] list content from s3",
			zap.String("name", name),
			zap.Int64("size", size),
			zap.Any("ddl", ddl))
	}
	var fileData []byte
	if size == 0 || size > maxDDLFlushSize {
		// no ddl file exists or
		// exists file is oversized. we should generate a new file
		fileData = data
		name = s.prefix + makeDDLFileObject(ddl.CommitTs)
		log.Debug("[EmitDDLEvent] create first or rotate ddl log",
			zap.String("name", name), zap.Any("ddl", ddl))
	} else {
		// hack way: append data to old file
		log.Debug("[EmitDDLEvent] append ddl to origin log",
			zap.String("name", name), zap.Any("ddl", ddl))
		obj, err := s.getObjectWithKey(ctx, name)
		if err != nil {
			return err
		}
		buf := make([]byte, maxDDLFlushSize)
		n, err := obj.Body.Read(buf)
		if err != nil {
			if err != io.EOF {
				return err
			}
		}
		fileData = append(buf[:n], '\n')
		fileData = append(fileData, data...)
	}
	return s.putObjectWithKey(ctx, name, fileData)
}

func (s *s3Sink) Initialize(ctx context.Context, tableInfo []*model.SimpleTableInfo) error {
	if tableInfo != nil {
		for _, table := range tableInfo {
			if table != nil {
				err := s.putObjectWithKey(ctx, s.prefix+makeTableDirectoryName(table.TableID), nil)
				if err != nil {
					return errors.Annotate(err, "create table directory on s3 failed")
				}
			}
		}
		// update log meta to record the relationship about tableName and tableID
		s.logMeta = makeLogMetaContent(tableInfo)

		data, err := s.logMeta.Marshal()
		if err != nil {
			return errors.Annotate(err, "marshal meta to json failed")
		}
		return s.putObjectWithKey(ctx, s.prefix+logMetaFile, data)
	}
	return nil
}

func (s *s3Sink) Close() error {
	return nil
}

// NewS3Sink creates new sink support log data to s3 directly
func NewS3Sink(sinkURI *url.URL) (*s3Sink, error) {
	if len(sinkURI.Host) == 0 {
		return nil, errors.Errorf("please specify the bucket for s3 in %s", sinkURI)
	}
	bucket := sinkURI.Host
	prefix := strings.Trim(sinkURI.Path, "/")
	prefix += "/"
	s3options := new(S3Options)
	err := s3options.
		extractFromQuery(sinkURI).
		adjust()
	if err != nil {
		return nil, errors.Annotatef(err, "parse s3 config failed with %s", sinkURI)
	}
	awsConfig := s3options.apply()
	awsSessionOpts := session.Options{Config: *awsConfig}
	ses, err := session.NewSessionWithOptions(awsSessionOpts)
	if err != nil {
		return nil, err
	}

	s3client := s3.New(ses)
	err = checkBucket(s3client, bucket)
	if err != nil {
		return nil, errors.Errorf("bucket %s is not accessible: %v", bucket, err)
	}

	notifyChan := make(chan struct{})
	tableBuffers := make([]*tableBuffer, 0)
	return &s3Sink{
		bucket:  bucket,
		prefix:  prefix,
		options: s3options,
		client:  s3client,

		tableBuffers: tableBuffers,
		notifyChan:   notifyChan,
	}, nil
}

// checkBucket checks if a path exists
var checkBucket = func(svc *s3.S3, bucket string) error {
	input := &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	}
	_, err := svc.HeadBucket(input)
	return err
}
