// Copyright (c) 2016 Danilo Bürger <info@danilobuerger.de>

// Package s3cache implements an autocert.Cache to store certificate data within an AWS S3 bucket.
//
// See https://godoc.org/golang.org/x/crypto/acme/autocert
package s3cache

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"golang.org/x/crypto/acme/autocert"
)

// Logger for outputing logs.
type Logger interface {
	Printf(format string, v ...interface{})
}

// Making sure that we're adhering to the autocert.Cache interface.
var _ autocert.Cache = (*Cache)(nil)

// Cache provides a s3 backend to the autocert cache.
type Cache struct {
	// Prefix is used to prefix every objects key cached in s3.
	Prefix string
	// Logger is used for debug logging.
	Logger Logger

	bucket string
	s3     s3iface.S3API
}

// New creates an s3 instance that can be used with autocert.Cache.
// It returns any errors that could happen while connecting to S3.
func New(region, bucket string) (*Cache, error) {
	sess, err := session.NewSession(&aws.Config{
		CredentialsChainVerboseErrors: aws.Bool(true),
		Region: aws.String(region),
	})
	if err != nil {
		return nil, err
	}

	return NewWithProvider(sess, bucket)
}

// NewWithProvider creates a new s3 autocert.Cache from a client.ConfigProvider.
func NewWithProvider(p client.ConfigProvider, bucket string) (*Cache, error) {
	return NewWithS3(s3.New(p), bucket)
}

// NewWithS3 creates a new s3 autocert.Cache from a s3iface.S3API.
func NewWithS3(s3 s3iface.S3API, bucket string) (*Cache, error) {
	return &Cache{
		bucket: bucket,
		s3:     s3,
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
	key = c.Prefix + key
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
	key = c.Prefix + key
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
	key = c.Prefix + key
	c.log("S3 Cache Delete %s", key)

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
