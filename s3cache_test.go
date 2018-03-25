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

// s3session is for overwriting the s3 interface during testing
func s3session(session s3iface.S3API) OptFunc {
	return func(o *options) {
		o.session = session
	}
}

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

func TestCacheWithOption(t *testing.T) {
	cache, err := New("", "my-bucket", s3session(&testS3{cache: map[string][]byte{}}))
	assert.NoError(t, err)
	ctx := context.Background()

	assert.Equal(t, cache.bucket, "my-bucket")

	_, err = cache.Get(ctx, "nonexistent")
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
	testCache := &testS3{cache: map[string][]byte{}}
	cache, err := New("", "my-bucket", s3session(testCache), KeyPrefix("/path/to/certs/here"))
	assert.NoError(t, err)
	ctx := context.Background()

	assert.Equal(t, cache.bucket, "my-bucket")

	_, err = cache.Get(ctx, "nonexistent")
	assert.Equal(t, autocert.ErrCacheMiss, err)

	b1 := []byte{1}
	assert.NoError(t, cache.Put(ctx, "dummy", b1))

	assert.Contains(t, testCache.cache, "/path/to/certs/here/dummy")

	b2, err := cache.Get(ctx, "dummy")
	assert.NoError(t, err)
	assert.Equal(t, b1, b2)

	assert.NoError(t, cache.Delete(ctx, "dummy"))

	_, err = cache.Get(ctx, "dummy")
	assert.Equal(t, autocert.ErrCacheMiss, err)
}

func TestCacheWithDSN(t *testing.T) {
	testCache := &testS3{cache: map[string][]byte{}}
	cache, err := NewDSN("s3://my-bucket/path/to/certs/here", s3session(testCache))
	assert.NoError(t, err)
	ctx := context.Background()

	assert.Equal(t, cache.bucket, "my-bucket")

	_, err = cache.Get(ctx, "nonexistent")
	assert.Equal(t, autocert.ErrCacheMiss, err)

	b1 := []byte{1}
	assert.NoError(t, cache.Put(ctx, "dummy", b1))

	assert.Contains(t, testCache.cache, "/path/to/certs/here/dummy")

	b2, err := cache.Get(ctx, "dummy")
	assert.NoError(t, err)
	assert.Equal(t, b1, b2)

	assert.NoError(t, cache.Delete(ctx, "dummy"))

	_, err = cache.Get(ctx, "dummy")
	assert.Equal(t, autocert.ErrCacheMiss, err)
}

func TestParseS3DSN(t *testing.T) {
	type rtn struct{ region, bucket, prefix string }
	var tests = []struct {
		name  string
		dsn   string
		want  rtn
		wante error
	}{
		{
			name: "bucket in path, no host",
			dsn:  "s3://example/dev/test/path",
			want: rtn{region: "us-east-1", bucket: "example", prefix: "/dev/test/path"},
		},
		{
			name: "bucket in path (looks like a domain), no host",
			dsn:  "s3://example.com/dev/test/path",
			want: rtn{region: "us-east-1", bucket: "example.com", prefix: "/dev/test/path"},
		},
		{
			name: "bucket in domain, no region",
			dsn:  "s3://example.com.s3.amazonaws.com/dev/test/path",
			want: rtn{region: "us-east-1", bucket: "example.com", prefix: "/dev/test/path"},
		},
		{
			name: "bucket in path, no region",
			dsn:  "s3://s3.amazonaws.com/example.com/dev/test/path",
			want: rtn{region: "us-east-1", bucket: "example.com", prefix: "/dev/test/path"},
		},
		{
			name: "bucket in path, with region",
			dsn:  "s3://s3-us-west-1.amazonaws.com/example.com/dev/test/path",
			want: rtn{region: "us-west-1", bucket: "example.com", prefix: "/dev/test/path"},
		},
		{
			name: "bucket in domain, with region",
			dsn:  "s3://example.com.s3-us-west-1.amazonaws.com/dev/test/path",
			want: rtn{region: "us-west-1", bucket: "example.com", prefix: "/dev/test/path"},
		},
		{
			name: "bucket in domain, with no region, no path",
			dsn:  "s3://example.com.s3.amazonaws.com/",
			want: rtn{region: "us-east-1", bucket: "example.com", prefix: "/"},
		},
	}

	for _, test := range tests {
		have, havee := parseS3DSN(test.dsn)
		if test.wante == nil && have.bucket != test.want.bucket {
			t.Errorf("%s:\nhave: %q want: %q", test.name, have.bucket, test.want.bucket)
		}

		if test.wante == nil && have.prefix != test.want.prefix {
			t.Errorf("%s:\nhave: %q want: %q", test.name, have.prefix, test.want.prefix)
		}

		if test.wante == nil && have.region != test.want.region {
			t.Errorf("%s:\nhave: %q want: %q", test.name, have.region, test.want.region)
		}

		if test.wante != nil {
			if havee != test.wante {
				t.Errorf(`%s:\nhave: "%v" want: "%v"`, test.name, havee, test.wante)
			}
		}
	}
}
