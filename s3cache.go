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
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"golang.org/x/crypto/acme/autocert"
)

// Logger logs.
type Logger interface {
	Printf(format string, v ...interface{})
}

var _ autocert.Cache = (*Cache)(nil)

// Cache provides an autocert s3 cache.
type Cache struct {
	bucket string
	s3     s3iface.S3API

	Logger Logger
}

// New creates a autocert s3 cache.
func New(region, bucket string) (*Cache, error) {
	sess, err := session.NewSession(&aws.Config{
		CredentialsChainVerboseErrors: aws.Bool(true),
		Region: aws.String(region),
	})
	if err != nil {
		return nil, err
	}

	return &Cache{
		bucket: bucket,
		s3:     s3.New(sess),
	}, nil
}

func (c *Cache) log(format string, v ...interface{}) {
	if c.Logger == nil {
		return
	}
	c.Logger.Printf(format, v)
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
	c.log("Cache Get %s", key)

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
	c.log("Cache Put %s", key)

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
