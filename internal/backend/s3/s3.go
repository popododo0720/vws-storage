// Package s3 is the object backend over an S3-compatible endpoint. Ceph RGW
// and MinIO are one driver. A Profile only picks defaults for the endpoint.
//
// Bytes never pass through this package. PresignPut and PresignGet hand the
// client a locator. The driver owns bucket lifecycle, Stat, and delete. Listing,
// request_id replay, and the md5 the API reports belong to the engine and store.
package s3

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"github.com/popododo0720/vws-storage/internal/backend"
	"github.com/popododo0720/vws-storage/internal/model"
)

// Profile names a set of endpoint defaults.
type Profile string

const (
	// RGW is Ceph RADOS Gateway.
	RGW Profile = "rgw"
	// MinIO is a MinIO server.
	MinIO Profile = "minio"
)

// Config is the connection to one endpoint.
type Config struct {
	Endpoint  string
	Region    string
	AccessKey string
	SecretKey string
	// PathStyle addresses buckets as /bucket instead of bucket.host.
	PathStyle bool
}

// ProfileConfig returns the defaults for p. The caller sets Endpoint and keys.
// Both profiles use path-style addressing, which needs no wildcard DNS.
func ProfileConfig(p Profile) (Config, error) {
	switch p {
	case RGW, MinIO:
		return Config{Region: "us-east-1", PathStyle: true}, nil
	default:
		return Config{}, fmt.Errorf("%w: s3 profile %q", model.ErrInvalidArgument, p)
	}
}

// Driver implements backend.Object and backend.Signer.
type Driver struct {
	c   *awss3.Client
	p   *awss3.PresignClient
	now func() time.Time
}

var (
	_ backend.Object = (*Driver)(nil)
	_ backend.Signer = (*Driver)(nil)
)

// Open builds the driver. It does not call the endpoint.
func Open(cfg Config) (*Driver, error) {
	if cfg.Endpoint == "" || cfg.Region == "" || cfg.AccessKey == "" || cfg.SecretKey == "" {
		return nil, fmt.Errorf("s3: endpoint, region, and keys are required")
	}
	c := awss3.New(awss3.Options{
		BaseEndpoint: aws.String(cfg.Endpoint),
		Region:       cfg.Region,
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		UsePathStyle: cfg.PathStyle,
		// MinIO and RGW do not all accept the SDK's default CRC32 additions.
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
	return &Driver{c: c, p: awss3.NewPresignClient(c), now: time.Now}, nil
}

// bucketID maps a VWS bucket name to a valid S3 bucket name. The VWS name has
// slashes and a project, so it is hashed.
func bucketID(b model.BucketName) string {
	sum := sha256.Sum256([]byte(b.String()))
	return "vws-" + hex.EncodeToString(sum[:20])
}

// objectKey puts the generation first. A generation is 16 hex characters and
// has no slash, so two refs never share a key.
func objectKey(ref model.ObjectRef) string {
	return ref.Generation.String() + "/" + ref.Path.Key.String()
}

func code(err error) string {
	var api smithy.APIError
	if errors.As(err, &api) {
		return api.ErrorCode()
	}
	return ""
}

// CreateBucket creates the bucket. A bucket these credentials already own is success.
func (d *Driver) CreateBucket(ctx context.Context, name model.BucketName) error {
	id := aws.String(bucketID(name))
	_, err := d.c.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: id})
	switch code(err) {
	case "BucketAlreadyOwnedByYou":
		return nil
	case "BucketAlreadyExists":
		// Some servers report an owned bucket this way. A bucket these
		// credentials can reach is ours, and one they cannot is someone else's.
		if _, herr := d.c.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: id}); herr == nil {
			return nil
		}
		return backend.Permanent(fmt.Errorf("%w: bucket", backend.ErrConflict))
	default:
		return err
	}
}

// DeleteBucket deletes the bucket. An absent bucket is success and a bucket
// that still holds objects is ErrBusy.
func (d *Driver) DeleteBucket(ctx context.Context, name model.BucketName) error {
	_, err := d.c.DeleteBucket(ctx, &awss3.DeleteBucketInput{Bucket: aws.String(bucketID(name))})
	switch code(err) {
	case "", "NoSuchBucket":
		return nil
	case "BucketNotEmpty":
		return backend.ErrBusy
	default:
		return err
	}
}

// Stat reports one generation. The SHA-256 is set only when the upload carried
// it. MD5 is the ETag, which equals the payload MD5 for a single-part upload
// without server-side encryption, and is zero otherwise.
func (d *Driver) Stat(ctx context.Context, ref model.ObjectRef) (model.ObservedObject, error) {
	out, err := d.c.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket:       aws.String(bucketID(ref.Path.Bucket)),
		Key:          aws.String(objectKey(ref)),
		ChecksumMode: "ENABLED",
	})
	switch code(err) {
	case "":
	case "NotFound", "NoSuchKey", "NoSuchBucket":
		return model.ObservedObject{}, backend.ErrNotFound
	default:
		return model.ObservedObject{}, err
	}
	obs := model.ObservedObject{
		Size:       model.Bytes(aws.ToInt64(out.ContentLength)),
		ETag:       model.ETag(strings.Trim(aws.ToString(out.ETag), `"`)),
		UploadedAt: aws.ToTime(out.LastModified),
	}
	if raw, err := base64.StdEncoding.DecodeString(aws.ToString(out.ChecksumSHA256)); err == nil && len(raw) == len(obs.SHA256) {
		copy(obs.SHA256[:], raw)
	}
	if m, err := model.ParseMD5(string(obs.ETag)); err == nil {
		obs.MD5 = m
	}
	return obs, nil
}

// DeleteObject deletes one generation. An absent generation is success.
func (d *Driver) DeleteObject(ctx context.Context, ref model.ObjectRef) error {
	_, err := d.c.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: aws.String(bucketID(ref.Path.Bucket)),
		Key:    aws.String(objectKey(ref)),
	})
	if code(err) == "NoSuchBucket" {
		return nil
	}
	return err
}

// PresignPut signs a PUT that fixes the body length, carries the SHA-256 when
// one is given so the server verifies it, and fails if the key exists.
// The returned headers must be sent with the request.
func (d *Driver) PresignPut(ctx context.Context, ref model.ObjectRef, size model.Bytes, sha model.Digest, expires time.Time) (model.Locator, error) {
	ttl, err := d.ttl(expires)
	if err != nil {
		return model.Locator{}, err
	}
	in := &awss3.PutObjectInput{
		Bucket:        aws.String(bucketID(ref.Path.Bucket)),
		Key:           aws.String(objectKey(ref)),
		ContentLength: aws.Int64(int64(size)),
		IfNoneMatch:   aws.String("*"),
	}
	if !sha.IsZero() {
		in.ChecksumSHA256 = aws.String(base64.StdEncoding.EncodeToString(sha[:]))
	}
	req, err := d.p.PresignPutObject(ctx, in, awss3.WithPresignExpires(ttl))
	if err != nil {
		return model.Locator{}, err
	}
	return d.locator(req, ttl), nil
}

// PresignGet signs a GET.
func (d *Driver) PresignGet(ctx context.Context, ref model.ObjectRef, expires time.Time) (model.Locator, error) {
	ttl, err := d.ttl(expires)
	if err != nil {
		return model.Locator{}, err
	}
	in := &awss3.GetObjectInput{Bucket: aws.String(bucketID(ref.Path.Bucket)), Key: aws.String(objectKey(ref))}
	req, err := d.p.PresignGetObject(ctx, in, awss3.WithPresignExpires(ttl))
	if err != nil {
		return model.Locator{}, err
	}
	return d.locator(req, ttl), nil
}

// ttl rounds down to whole seconds so the URL never outlives expires.
func (d *Driver) ttl(expires time.Time) (time.Duration, error) {
	ttl := expires.Sub(d.now()).Truncate(time.Second)
	if ttl < time.Second {
		return 0, backend.Permanent(fmt.Errorf("%w: expiry is not in the future", backend.ErrInvalid))
	}
	return ttl, nil
}

func (d *Driver) locator(req *v4.PresignedHTTPRequest, ttl time.Duration) model.Locator {
	loc := model.Locator{Method: req.Method, URL: req.URL, ExpiresAt: d.now().Add(ttl)}
	for k, v := range req.SignedHeader {
		if http.CanonicalHeaderKey(k) == "Host" || len(v) == 0 {
			continue
		}
		if loc.Headers == nil {
			loc.Headers = map[string]string{}
		}
		loc.Headers[k] = v[0]
	}
	return loc
}
