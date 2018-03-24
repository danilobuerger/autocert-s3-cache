// Copyright (c) 2016 Danilo Bürger <info@danilobuerger.de>

// package s3cache implements a https://godoc.org/golang.org/x/crypto/acme/autocert Cache to store keys within in a S3 bucket. If the key does not exist, it will be created automatically.
package s3cache

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
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
	session s3iface.S3API
}

// returns an options object with defaults
func newOptions() *options {
	o := new(options)

	// defaults
	o.prefix = "/"
	return o
}

// KeyPrefix adds an S3 key prefix to the certs stored
func KeyPrefix(prefix string) OptFunc {
	return func(o *options) {
		o.prefix = strings.TrimRight(prefix, "/") + "/"
	}
}

// s3session is for overwriting the s3 interface during testing
func s3session(session s3iface.S3API) OptFunc {
	return func(o *options) {
		o.session = session
	}
}

// New creates an s3 interface to the autocert cache.
func New(region, bucket string, opts ...OptFunc) (*Cache, error) {

	option := newOptions()
	for _, opt := range opts {
		opt(option)
	}

	if option.session == nil {
		sess, err := session.NewSession(&aws.Config{
			CredentialsChainVerboseErrors: aws.Bool(true),
			Region: aws.String(region),
		})
		if err != nil {
			return nil, err
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
