// Copyright (c) 2016 Danilo Bürger <info@danilobuerger.de>

package s3cache

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/stretchr/testify/assert"
	"golang.org/x/crypto/acme/autocert"
	"golang.org/x/net/context"
)

type testLogger struct {
	called bool
}

func (l *testLogger) Printf(format string, v ...interface{}) {
	l.called = true
}

func TestLogger(t *testing.T) {
	c := &Cache{}
	assert.NotPanics(t, func() {
		c.log("")
	})

	l := &testLogger{}
	c.Logger = l
	assert.False(t, l.called)
	c.log("")
	assert.True(t, l.called)
}

type testS3 struct {
	s3iface.S3API
	cache map[string][]byte
}

func (t *testS3) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	b, ok := t.cache[*input.Key]
	if !ok {
		return nil, awserr.NewRequestFailure(nil, http.StatusNotFound, "")
	}

	return &s3.GetObjectOutput{
		Body: ioutil.NopCloser(bytes.NewReader(b)),
	}, nil
}

func (t *testS3) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	b, err := ioutil.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}

	t.cache[*input.Key] = b
	return &s3.PutObjectOutput{}, nil
}

func (t *testS3) DeleteObject(input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	delete(t.cache, *input.Key)
	return &s3.DeleteObjectOutput{}, nil
}

func TestCache(t *testing.T) {
	cache := &Cache{s3: &testS3{cache: map[string][]byte{}}}
	ctx := context.Background()

	_, err := cache.Get(ctx, "nonexistent")
	assert.Equal(t, autocert.ErrCacheMiss, err)

	b1 := []byte{1}
	assert.NoError(t, cache.Put(ctx, "dummy", b1))

	b2, err := cache.Get(ctx, "dummy")
	assert.NoError(t, err)
	assert.Equal(t, b1, b2)

	assert.NoError(t, cache.Delete(ctx, "dummy"))

	_, err = cache.Get(ctx, "dummy")
	assert.Equal(t, autocert.ErrCacheMiss, err)
}

func TestCacheWithPrefix(t *testing.T) {
	testS3Cache := &testS3{cache: map[string][]byte{}}
	cache := &Cache{bucket: "my-bucket", s3: testS3Cache}
	cache.Prefix = "/path/to/certs/here/"
	ctx := context.Background()

	assert.Equal(t, cache.bucket, "my-bucket")

	_, err := cache.Get(ctx, "nonexistent")
	assert.Equal(t, autocert.ErrCacheMiss, err)

	b1 := []byte{1}
	assert.NoError(t, cache.Put(ctx, "dummy", b1))

	assert.Contains(t, testS3Cache.cache, "/path/to/certs/here/dummy")

	b2, err := cache.Get(ctx, "dummy")
	assert.NoError(t, err)
	assert.Equal(t, b1, b2)

	assert.NoError(t, cache.Delete(ctx, "dummy"))

	_, err = cache.Get(ctx, "dummy")
	assert.Equal(t, autocert.ErrCacheMiss, err)
}
