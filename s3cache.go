// Copyright (c) 2016 Danilo Bürger <info@danilobuerger.de>

// package s3cache implements a https://godoc.org/golang.org/x/crypto/acme/autocert Cache to store keys within in a S3 bucket. If the key does not exist, it will be created automatically.
package s3cache

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"golang.org/x/crypto/acme/autocert"
)

// Logger for outputing logs
type Logger interface {
	Printf(format string, v ...interface{})
}

// Making sure that we're adhering to the autocert.Cache interface
var _ autocert.Cache = (*Cache)(nil)

// Cache provides a s3 backend to the autocert cache.
type Cache struct {
	bucket string
	prefix string
	s3     s3iface.S3API

	Logger Logger
}

// OptFunc is for options passed in to New
type OptFunc func(*options)

// options holds options that can be used while making a new Cache object
type options struct {
	prefix  string
	id      string
	secret  string
	session s3iface.S3API
}

// returns an options object with defaults
func newOptions() *options {
	o := new(options)

	// defaults
	o.prefix = "/"
	return o
}

func UserPass(id, secret string) OptFunc {
	return func(o *options) {
		o.id = id
		o.secret = secret
	}
}

// KeyPrefix adds an S3 key prefix to the certs stored
func KeyPrefix(prefix string) OptFunc {
	return func(o *options) {
		o.prefix = strings.TrimRight(prefix, "/") + "/"
	}
}

type dsnParts struct {
	id, secret             string
	region, bucket, prefix string
}

// parseS3DSN can parse the following URIs
// the s3-<region> is always optional.
//
// s3://<bucket>.s3-<region>.amazonaws.com/<key>
// s3://<bucket>.s3.amazonaws.com/<key>
// s3://s3.amazonaws.com/<bucket>/<key>
// s3://s3-<region>.amazonaws.com/<bucket>/<key>
// s3://<username>:@<bucket>.s3.amazonaws.com/<key>
// s3://<username>:@s3.amazonaws.com/<bucket>/<key>
// s3://<username>:<password>@<bucket>.s3.amazonaws.com/<key>
// s3://<username>:<password>@s3.amazonaws.com/<bucket>/<key>
func parseS3DSN(dsn string) (dsnParts, error) {
	rtn := dsnParts{}
	rtn.region = "us-east-1" // default

	u, err := url.Parse(dsn)
	if err != nil {
		return rtn, fmt.Errorf("url parse: %v", err)
	}

	if strings.ToLower(u.Scheme) != "s3" {
		return rtn, fmt.Errorf("expecting a s3:// scheme")
	}

	if u.User != nil {
		rtn.id = u.User.Username()
		rtn.secret, _ = u.User.Password()
	}

	hparts := strings.Split(u.Hostname(), ".")

	// check to see if its just a bucket
	if len(hparts) == 1 {
		rtn.bucket = hparts[0]
	}

	// check to see if its a s3 domain
	// (where the bucket can be attached as a subdomain)

	if len(hparts) >= 2 {
		if strings.Join(hparts[len(hparts)-2:], ".") != "amazonaws.com" {
			rtn.bucket = u.Hostname()
		} else {
			// check for region
			switch region := hparts[len(hparts)-3]; region {
			case "s3":
				// nothing....
			default:
				if len(region) > 3 && region[:3] == "s3-" {
					rtn.region = region[3:]
				} else {
					return rtn, fmt.Errorf("the region doesn't appear to be correct")
				}
			}

			// check for bucket name in domain
			if len(hparts[:len(hparts)-2]) > 1 {
				rtn.bucket = strings.Join(hparts[:len(hparts)-3], ".")
			}
		}
	}

	rtn.prefix = u.Path
	if len(rtn.bucket) == 0 {
		slash := strings.Index(rtn.prefix[1:], "/")
		rtn.bucket = (rtn.prefix[1:])[:slash]
		rtn.prefix = rtn.prefix[slash+1:]
	}

	return rtn, nil
}

// NewDSN returns a s3 interface to the autocert cache based
// on a standard S3 DSN. The path cannot be overwritten
func NewDSN(dsn string, opts ...OptFunc) (*Cache, error) {
	p, err := parseS3DSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parsing DSN: %v", err)
	}

	// pass in the access id and secret first so that
	// they can be overwritten
	opts = append([]OptFunc{UserPass(p.id, p.secret), KeyPrefix(p.prefix)}, opts...)
	return New(p.region, p.bucket, opts...)
}

// New creates an s3 interface to the autocert cache.
func New(region, bucket string, opts ...OptFunc) (*Cache, error) {

	option := newOptions()
	for _, opt := range opts {
		opt(option)
	}

	if option.session == nil {
		awsOptions := &aws.Config{
			CredentialsChainVerboseErrors: aws.Bool(true),
			Region: aws.String(region),
		}
		if len(option.id) > 0 || len(option.secret) > 0 {
			awsOptions = awsOptions.WithCredentials(
				credentials.NewStaticCredentials(option.id, option.secret, ""),
			)
		}
		sess, err := session.NewSession(awsOptions)
		if err != nil {
			return nil, fmt.Errorf("AWS session: %v", err)
		}
		option.session = s3.New(sess)
	}

	return &Cache{
		bucket: bucket,
		prefix: option.prefix,
		s3:     option.session,
	}, nil
}

func (c *Cache) log(format string, v ...interface{}) {
	if c.Logger == nil {
		return
	}
	c.Logger.Printf(format, v...)
}

func (c *Cache) get(key string) ([]byte, error) {
	resp, err := c.s3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return ioutil.ReadAll(resp.Body)
}

// Get returns a certificate data for the specified key.
func (c *Cache) Get(ctx context.Context, key string) ([]byte, error) {
	key = c.prefix + key
	c.log("S3 Cache Get %s", key)

	var (
		data []byte
		err  error
		done = make(chan struct{})
	)

	go func() {
		data, err = c.get(key)
		close(done)
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-done:
	}

	if awsErr, ok := err.(awserr.RequestFailure); ok {
		if awsErr.StatusCode() == http.StatusNotFound {
			return nil, autocert.ErrCacheMiss
		}
	}

	return data, err
}

func (c *Cache) put(key string, data []byte) error {
	_, err := c.s3.PutObject(&s3.PutObjectInput{
		Bucket:               aws.String(c.bucket),
		Key:                  aws.String(key),
		Body:                 bytes.NewReader(data),
		ServerSideEncryption: aws.String("AES256"),
	})
	return err
}

// Put stores the data in the cache under the specified key.
func (c *Cache) Put(ctx context.Context, key string, data []byte) error {
	key = c.prefix + key
	c.log("S3 Cache Put %s", key)

	var (
		err  error
		done = make(chan struct{})
	)

	go func() {
		err = c.put(key, data)
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	}

	return err
}

func (c *Cache) delete(key string) error {
	_, err := c.s3.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	return err
}

// Delete removes a certificate data from the cache under the specified key.
func (c *Cache) Delete(ctx context.Context, key string) error {
	key = c.prefix + key
	c.log("Cache Delete %s", key)

	var (
		err  error
		done = make(chan struct{})
	)

	go func() {
		err = c.delete(key)
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	}

	return err
}
