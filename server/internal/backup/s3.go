package backup

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	tmtypes "github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Store is the bucket a cluster's backups go to.
type Store struct {
	Config *Config
	client *s3.Client
}

// NewStore returns the store c names, with credentials from the AWS SDK's
// chain.
func NewStore(ctx context.Context, c *Config) (*Store, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if c.Region != "" {
		opts = append(opts, awsconfig.WithRegion(c.Region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
			o.UsePathStyle = true
		}
	})
	return &Store{Config: c, client: client}, nil
}

// Client is the store's S3 client, for tests and tools.
func (s *Store) Client() *s3.Client {
	return s.client
}

// Upload writes r to key, in parts as it comes, locked in compliance mode
// until ret.Until and tagged with its tier. A put-only identity can do it:
// s3:PutObject, s3:PutObjectRetention, s3:PutObjectTagging and, for a
// failed upload, s3:AbortMultipartUpload.
func (s *Store) Upload(ctx context.Context, key string, r io.Reader, ret Retention) error {
	_, err := transfermanager.New(s.client).UploadObject(ctx, &transfermanager.UploadObjectInput{
		Bucket:                    aws.String(s.Config.Bucket),
		Key:                       aws.String(key),
		Body:                      r,
		ObjectLockMode:            tmtypes.ObjectLockModeCompliance,
		ObjectLockRetainUntilDate: aws.Time(ret.Until),
		Tagging:                   aws.String("tier=" + ret.Tier),
		// Object Lock needs a checksum of every part.
		ChecksumAlgorithm: tmtypes.ChecksumAlgorithmCrc32,
	})
	return err
}

// Put writes b to key, as Upload does.
func (s *Store) Put(ctx context.Context, key string, b []byte, ret Retention) error {
	return s.Upload(ctx, key, bytes.NewReader(b), ret)
}

// JournalKey is the object of one batch of a node's journal, uploaded at t.
func (s *Store) JournalKey(node string, t time.Time) string {
	return s.Config.JournalKey(node, t)
}

// Get returns the object at key. It needs a reader's identity.
func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.Config.Bucket), Key: aws.String(key)})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

// List returns the keys under prefix. It needs a reader's identity.
func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	p := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{Bucket: aws.String(s.Config.Bucket), Prefix: aws.String(prefix)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
	}
	return keys, nil
}
