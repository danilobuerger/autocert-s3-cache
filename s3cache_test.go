package s3cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
