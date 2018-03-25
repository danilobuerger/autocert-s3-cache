[![GoDoc](https://godoc.org/github.com/danilobuerger/autocert-s3-cache?status.svg)](https://godoc.org/github.com/danilobuerger/autocert-s3-cache) [![Build Status](https://travis-ci.org/danilobuerger/autocert-s3-cache.svg?branch=master)](https://travis-ci.org/danilobuerger/autocert-s3-cache) [![Coverage Status](https://coveralls.io/repos/github/danilobuerger/autocert-s3-cache/badge.svg?branch=master)](https://coveralls.io/github/danilobuerger/autocert-s3-cache?branch=master) [![Go Report Card](https://goreportcard.com/badge/github.com/danilobuerger/autocert-s3-cache)](https://goreportcard.com/report/github.com/danilobuerger/autocert-s3-cache)

# autocert-s3-cache

AWS S3 cache for [acme/autocert](https://godoc.org/golang.org/x/crypto/acme/autocert) written in Go.

## Example

```go
cache, err := s3cache.New("eu-west-1", "my-bucket")
if err != nil {
  // Handle error
}

m := autocert.Manager{
  Prompt:     autocert.AcceptTOS,
  HostPolicy: autocert.HostWhitelist("example.org"),
  Cache:      cache,
}

s := &http.Server{
  Addr:      ":https",
  TLSConfig: &tls.Config{GetCertificate: m.GetCertificate},
}

s.ListenAndServeTLS("", "")
```
