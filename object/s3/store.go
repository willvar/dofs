package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/willvar/dofs"
)

type Config struct {
	Endpoint     string
	AccessKey    string
	SecretKey    string
	SessionToken string
	Bucket       string
	Region       string
	Prefix       string
	Secure       bool
}

type Store struct {
	client *minio.Client
	bucket string
	prefix string
}

func (s *Store) Check(ctx context.Context) error {
	exists, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return mapError(err)
	}
	if !exists {
		return fmt.Errorf("dofs s3: bucket %q does not exist", s.bucket)
	}
	return nil
}

func New(config Config) (*Store, error) {
	if strings.TrimSpace(config.Endpoint) == "" || strings.TrimSpace(config.Bucket) == "" {
		return nil, errors.New("dofs s3: endpoint and bucket are required")
	}
	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKey, config.SecretKey, config.SessionToken),
		Secure: config.Secure,
		Region: config.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("create DOFS S3 client: %w", err)
	}
	return NewWithClient(client, config.Bucket, config.Prefix)
}

func NewWithClient(client *minio.Client, bucket, prefix string) (*Store, error) {
	if client == nil || strings.TrimSpace(bucket) == "" {
		return nil, errors.New("dofs s3: client and bucket are required")
	}
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if prefix == "." || prefix == ".." || strings.Contains(prefix, "\x00") {
		return nil, errors.New("dofs s3: invalid object prefix")
	}
	return &Store{client: client, bucket: bucket, prefix: prefix}, nil
}

func (s *Store) physicalKey(key string) (string, error) {
	return s.physicalPath(key, false)
}

func (s *Store) physicalPrefix(prefix string) (string, error) {
	return s.physicalPath(prefix, true)
}

func (s *Store) physicalPath(value string, allowTrailingSlash bool) (string, error) {
	if value == "" || strings.HasPrefix(value, "/") || strings.Contains(value, "\x00") {
		return "", errors.New("dofs s3: invalid object key")
	}
	trailingSlash := strings.HasSuffix(value, "/")
	if trailingSlash && !allowTrailingSlash {
		return "", errors.New("dofs s3: invalid object key")
	}
	base := strings.TrimSuffix(value, "/")
	clean := path.Clean(base)
	if clean != base || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", errors.New("dofs s3: invalid object key")
	}
	if trailingSlash {
		clean += "/"
	}
	if s.prefix == "" {
		return clean, nil
	}
	return s.prefix + "/" + clean, nil
}

func (s *Store) GetRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	physical, err := s.physicalKey(key)
	if err != nil {
		return nil, err
	}
	if start < 0 || end < start {
		return nil, errors.New("dofs s3: invalid object range")
	}
	options := minio.GetObjectOptions{}
	if err := options.SetRange(start, end); err != nil {
		return nil, fmt.Errorf("set S3 object range: %w", err)
	}
	object, err := s.client.GetObject(ctx, s.bucket, physical, options)
	if err != nil {
		return nil, mapError(err)
	}
	return &mappedReadCloser{reader: object}, nil
}

func (s *Store) Stat(ctx context.Context, key string) (dofs.ObjectInfo, error) {
	physical, err := s.physicalKey(key)
	if err != nil {
		return dofs.ObjectInfo{}, err
	}
	info, err := s.client.StatObject(ctx, s.bucket, physical, minio.StatObjectOptions{})
	if err != nil {
		return dofs.ObjectInfo{}, mapError(err)
	}
	if info.Size < 0 {
		return dofs.ObjectInfo{}, errors.New("dofs s3: invalid object size")
	}
	return dofs.ObjectInfo{Size: info.Size}, nil
}

type mappedReadCloser struct {
	reader io.ReadCloser
}

func (r *mappedReadCloser) Read(buffer []byte) (int, error) {
	count, err := r.reader.Read(buffer)
	return count, mapError(err)
}

func (r *mappedReadCloser) Close() error {
	return mapError(r.reader.Close())
}

func (s *Store) Put(ctx context.Context, key string, source io.Reader, size int64) error {
	physical, err := s.physicalKey(key)
	if err != nil {
		return err
	}
	if size < 0 {
		return errors.New("dofs s3: invalid object size")
	}
	// If-None-Match on PutObject is a MinIO extension, not a portable S3
	// operation; Aliyun OSS and other compatible providers may reject it. DOFS
	// generation keys contain a fresh random transaction ID and metadata is
	// published with a generation CAS, so an existence check preserves retry
	// semantics without coupling the ordinary-I/O path to one provider.
	if _, err := s.client.StatObject(ctx, s.bucket, physical, minio.StatObjectOptions{}); err == nil {
		return dofs.ErrAlreadyExists
	} else if !errors.Is(mapError(err), dofs.ErrObjectNotFound) {
		return mapError(err)
	}
	options := minio.PutObjectOptions{
		ContentType: "application/vnd.dofs.encrypted",
	}
	_, err = s.client.PutObject(ctx, s.bucket, physical, source, size, options)
	return mapError(err)
}

func (s *Store) Copy(ctx context.Context, sourceKey, destinationKey string) error {
	source, err := s.physicalKey(sourceKey)
	if err != nil {
		return err
	}
	destination, err := s.physicalKey(destinationKey)
	if err != nil {
		return err
	}
	if source == destination {
		return dofs.ErrAlreadyExists
	}
	if _, err := s.client.StatObject(ctx, s.bucket, destination, minio.StatObjectOptions{}); err == nil {
		return dofs.ErrAlreadyExists
	} else if !errors.Is(mapError(err), dofs.ErrObjectNotFound) {
		return mapError(err)
	}
	_, err = s.client.CopyObject(ctx,
		minio.CopyDestOptions{Bucket: s.bucket, Object: destination},
		minio.CopySrcOptions{Bucket: s.bucket, Object: source},
	)
	return mapError(err)
}

func (s *Store) Delete(ctx context.Context, key string) error {
	physical, err := s.physicalKey(key)
	if err != nil {
		return err
	}
	return mapError(s.client.RemoveObject(ctx, s.bucket, physical, minio.RemoveObjectOptions{}))
}

func (s *Store) DeletePrefix(ctx context.Context, prefix string) error {
	physical, err := s.physicalPrefix(prefix)
	if err != nil {
		return err
	}
	const batchSize = 1000
	batch := make([]minio.ObjectInfo, 0, batchSize)
	removeBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		objects := make(chan minio.ObjectInfo, len(batch))
		for _, object := range batch {
			objects <- minio.ObjectInfo{Key: object.Key}
		}
		close(objects)
		for failure := range s.client.RemoveObjects(ctx, s.bucket, objects, minio.RemoveObjectsOptions{}) {
			if failure.Err != nil {
				return mapError(failure.Err)
			}
		}
		batch = batch[:0]
		return nil
	}
	for object := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: physical, Recursive: true}) {
		if object.Err != nil {
			return mapError(object.Err)
		}
		batch = append(batch, object)
		if len(batch) == batchSize {
			if err := removeBatch(); err != nil {
				return err
			}
		}
	}
	if err := removeBatch(); err != nil {
		return err
	}
	return context.Cause(ctx)
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	response := minio.ToErrorResponse(err)
	switch response.Code {
	case "NoSuchKey", "NoSuchObject", "NotFound":
		return fmt.Errorf("%w: %v", dofs.ErrObjectNotFound, err)
	case "PreconditionFailed":
		return fmt.Errorf("%w: %v", dofs.ErrAlreadyExists, err)
	default:
		return err
	}
}

var _ dofs.ObjectStore = (*Store)(nil)
