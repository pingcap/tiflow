package logBackup

import (
	"bytes"
	"context"
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
	"github.com/pingcap/ticdc/cdc/sink/codec"
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

	maxBufferKeys = 10
	maxFlushSize = 96
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

type chunk struct {
	hash int
	multiPartUpload *s3.CreateMultipartUploadOutput
	partNumber int
}

type s3Sink struct {
	bucket string
	prefix string

	options *S3Options
	client  *s3.S3

	logMeta *LogMeta
	flushLogMetaTicker *time.Ticker

	idChunkMap    sync.Map
	bufferChan    []chan *model.RowChangedEvent
	bufferKeySize []int64
	notifyChan    chan *chunk
	uploadParts []struct {
		byteSize int64
		completeParts []*s3.CompletedPart
	}

	newEncoder func() codec.EventBatchEncoder
}

func (s *s3Sink) putObject(ctx context.Context, name string, data []byte) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.prefix + name),
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
		Key:    aws.String(s.prefix + name),
	}
	return s.client.WaitUntilObjectExistsWithContext(ctx, hInput)
}

func (s *s3Sink) createMultipartUpload(path string) (*s3.CreateMultipartUploadOutput, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(path),
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
	} else {
		return &s3.CompletedPart{
			ETag:       uploadResult.ETag,
			PartNumber: aws.Int64(int64(partNumber)),
		}, nil
	}
}

func (s *s3Sink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	log.Info("emit row change events", zap.Any("rows", rows))
	var (
		err error
		resp *s3.CreateMultipartUploadOutput
	)
	for _, row := range rows {
		tableID := row.Table.GetTableID()
		var (
			ok bool
			i interface{}
		)
		if i, ok = s.idChunkMap.Load(tableID); !ok {
			// TODO merge construct
			s.bufferChan = append(s.bufferChan, make(chan *model.RowChangedEvent))
			s.bufferKeySize = append(s.bufferKeySize, 0)
			s.uploadParts = append(s.uploadParts, struct {
				byteSize      int64
				completeParts []*s3.CompletedPart
			}{byteSize: 0, completeParts: nil})
			path := makeTableFileName(tableID, row.CommitTs)
			resp, err = s.createMultipartUpload(path)
			if err != nil {
				return err
			}
			c := new(chunk)
			c.hash = len(s.bufferChan) - 1
			c.multiPartUpload= resp
			c.partNumber = -1
			i = c
			s.idChunkMap.Store(tableID, c)
		}
		chk := i.(*chunk)
		hash := chk.hash
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.bufferChan[hash] <- row:
			if s.bufferKeySize[hash] > maxBufferKeys {
				chk.partNumber ++
				s.idChunkMap.LoadOrStore(tableID, chk)
				s.notifyChan <- chk
			}
			s.bufferKeySize[hash] += int64(len(row.Keys))
		}
	}
	return nil
}

func (s *s3Sink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) error {
	log.Info("flush row change events", zap.Uint64("resolvedTs", resolvedTs))
	select {
	case n := <- s.notifyChan:
		encoder := s.newEncoder()
		for row := range s.bufferChan[n.hash] {
			_, err := encoder.AppendRowChangedEvent(row)
			if err != nil {
				return err
			}
		}
		key, _ := encoder.Build()
		hashPart := s.uploadParts[n.hash]
		if hashPart.byteSize > maxFlushSize {
			_, err := s.completeMultipartUpload(n.multiPartUpload, hashPart.completeParts)
			if err != nil {
				return err
			}
			s.uploadParts[n.hash].byteSize = 0
		} else {
			// TODO upload together with key and values
			completePart, err := s.uploadPart(n.multiPartUpload, key, n.partNumber)
			if err != nil {
				return err
			}
			hashPart.byteSize += int64(len(key))
			hashPart.completeParts = append(hashPart.completeParts, completePart)
		}
	case <- time.After(10 * time.Second):
		// TODO flush all buffer channel to file
	}
	return nil
}

func (s *s3Sink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	log.Info("emit checkpoint ts", zap.Uint64("ts", ts))
	select {
	case <- s.flushLogMetaTicker.C:
		s.logMeta.globalResolvedTS = ts
		data, err := s.logMeta.Marshal()
		if err != nil {
			return errors.Annotate(err, "marshal meta to json failed")
		}
		// TODO test whether putObject will cover old file
		return s.putObject(ctx, logMetaFile, data)
	}
	return nil
}

func (s *s3Sink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	encoder := s.newEncoder()
	op, err := encoder.AppendDDLEvent(ddl)
	if err != nil {
		return errors.Trace(err)
	}

	if op == codec.EncoderNoOperation {
		log.Info("no operation")
		return nil
	}

	key, value := encoder.Build()
	log.Info("emit ddl event", zap.ByteString("key", key), zap.ByteString("value", value))
	// TODO upload ddl with multiPartUpload
	return nil
}

func (s *s3Sink) Initialize(ctx context.Context, tableInfo []*model.TableInfo) error {
	if tableInfo != nil {
		for _, table := range tableInfo {
			if table != nil {
				err := s.putObject(ctx, makeTableDirectoryName(table.TableID), nil)
				if err != nil {
				}
				return errors.Annotate(err, "create table directory on s3 failed")
			}
		}
		// update log meta to record the relationship about tableName and tableID
		s.logMeta = makeLogMetaContent(tableInfo)
		s.flushLogMetaTicker = time.NewTicker(5 * time.Minute)

		data, err := s.logMeta.Marshal()
		if err != nil {
			return errors.Annotate(err, "marshal meta to json failed")
		}
		return s.putObject(ctx, logMetaFile, data)
	}
	return nil
}

func (s *s3Sink) Close() error {
	return nil
}

func NewS3Sink(ctx context.Context, sinkURI *url.URL) (*s3Sink, error) {
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

	v, _ := ses.Config.Credentials.Get()
	log.Info("sse", zap.String("access key", v.AccessKeyID), zap.String("secret access key", v.SecretAccessKey))

	s3client := s3.New(ses)
	err = checkS3PathExists(s3client, bucket, "")
	if err != nil {
		return nil, errors.Errorf("bucket %s is not accessible: %v", bucket, err)
	}

	bufferChan := make([]chan *model.RowChangedEvent, 0, 128)
	bufferKeySize := make([]int64, 0, 128)

	notifyChan := make(chan *chunk, 128)
	uploadParts := make([]struct {
		byteSize int64
		completeParts []*s3.CompletedPart
	}, 128)

	e := codec.NewJSONEventBatchEncoder
	return &s3Sink{
		bucket:  bucket,
		prefix:  prefix,
		options: s3options,
		client:  s3client,

		bufferChan: bufferChan,
		bufferKeySize: bufferKeySize,

		notifyChan: notifyChan,
		uploadParts: uploadParts,

		newEncoder: e,
	}, nil
}

// checkBucket checks if a path exists
var checkS3PathExists = func(svc *s3.S3, bucket string, key string) error {
	if len(key) == 0 {
		input := &s3.HeadBucketInput{
			Bucket: aws.String(bucket),
		}
		_, err := svc.HeadBucket(input)
		return err
	} else {
		input := &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key: aws.String(key),
		}
		_, err := svc.HeadObject(input)
		return err
	}
}
