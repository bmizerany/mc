package mc

import (
	"bytes"
	"github.com/bmizerany/assert"
	"net"
	"runtime"
	"testing"
)

const mcAddr = "localhost:11211"

// If need test auth, set true
const testAuth = false
const username = "mcgo"
const password = "foo"

func getConn(t *testing.T) (cn *Conn) {
	nc, err := net.Dial("tcp", mcAddr)
	assert.Equalf(t, nil, err, "%v", err)

	cn = &Conn{rwc: nc, buf: new(bytes.Buffer)}

	if runtime.GOOS != "darwin" && testAuth {
		println("Not on Darwin, testing auth")
		err = cn.Auth(username, password)
		assert.Equalf(t, nil, err, "%v", err)
	}

	return
}

func TestMCSimple(t *testing.T) {

	cn := getConn(t)

	err := cn.Del("foo")
	if err != ErrNotFound {
		assert.Equalf(t, nil, err, "%v", err)
	}

	_, _, _, err = cn.Get("foo")
	assert.Equalf(t, ErrNotFound, err, "%v", err)

	err = cn.Set("foo", "bar", 0, 0, 0)
	assert.Equalf(t, nil, err, "%v", err)

	// unconditional SET
	err = cn.Set("foo", "bar", 0, 0, 0)
	assert.Equalf(t, nil, err, "%v", err)

	err = cn.Set("foo", "bar", 1, 0, 0)
	assert.Equalf(t, ErrKeyExists, err, "%v", err)

	v, _, _, err := cn.Get("foo")
	assert.Equalf(t, nil, err, "%v", err)
	assert.Equal(t, "bar", v)

	err = cn.Set("n", "0", 0, 0, 0)
	assert.Equalf(t, nil, err, "%v", err)

	n, cas, err := cn.Incr("n", 1, 0, 0)
	assert.Equalf(t, nil, err, "%v", err)
	assert.NotEqual(t, 0, cas)
	assert.Equal(t, 1, n)

	n, cas, err = cn.Incr("n", 1, 0, 0)
	assert.Equalf(t, nil, err, "%v", err)
	assert.NotEqual(t, 0, cas)
	assert.Equal(t, 2, n)

	n, cas, err = cn.Decr("n", 1, 0, 0)
	assert.Equalf(t, nil, err, "%v", err)
	assert.NotEqual(t, 0, cas)
	assert.Equal(t, 1, n)

	cn.Del("foo")
	cn.Del("n")
}

func TestAppendPrepend(t *testing.T) {
	cn := getConn(t)

	cn.Del("foo")

	err := cn.Append("foo", "_bar", 0, 0, 0)
	assert.Equalf(t, ErrValueNotStored, err, "%v", err)

	err = cn.Prepend("foo", "bar_", 0, 0, 0)
	assert.Equalf(t, ErrValueNotStored, err, "%v", err)

	err = cn.Set("foo", "foo", 0, 0, 0)
	assert.Equalf(t, nil, err, "%v", err)

	err = cn.Append("foo", "_bar", 0, 0, 0)
	assert.Equalf(t, nil, err, "%v", err)

	value, _, _, err := cn.Get("foo")
	assert.Equalf(t, nil, err, "%v", err)
	assert.Equal(t, "foo_bar", value)

	err = cn.Prepend("foo", "bar_", 0, 0, 0)
	assert.Equalf(t, nil, err, "%v", err)

	value, _, _, err = cn.Get("foo")
	assert.Equalf(t, nil, err, "%v", err)
	assert.Equal(t, "bar_foo_bar", value)

	cn.Del("foo")
}

func TestFlush(t *testing.T) {

	cn := getConn(t)

	err := cn.Set("foo", "bar", 0, 0, 0)
	assert.Equalf(t, nil, err, "%v", err)

	value, _, _, err := cn.Get("foo")
	assert.Equalf(t, nil, err, "%v", err)
	assert.Equal(t, "bar", value)

	cn.Flush(0)
	_, _, _, err = cn.Get("foo")
	assert.Equalf(t, ErrNotFound, err, "%v", err)

	cn.Del("foo")
}

func TestAdd(t *testing.T) {
	cn := getConn(t)

	cn.Del("foo")

	err := cn.Add("foo", "bar", 0, 0, 0)
	assert.Equalf(t, nil, err, "%v", err)

	value, _, _, err := cn.Get("foo")
	assert.Equalf(t, nil, err, "%v", err)
	assert.Equal(t, "bar", value)

	err = cn.Add("foo", "bar", 0, 0, 0)
	assert.Equalf(t, ErrKeyExists, err, "%v", err)

	cn.Del("foo")
}

func TestReplace(t *testing.T) {
	cn := getConn(t)

	cn.Del("foo")

	err := cn.Replace("foo", "bar", 0, 0, 0)
	assert.Equalf(t, ErrNotFound, err, "%v", err)

	err = cn.Set("foo", "foo", 0, 0, 0)
	assert.Equalf(t, nil, err, "%v", err)

	err = cn.Replace("foo", "bar", 0, 0, 0)
	assert.Equalf(t, nil, err, "%v", err)

	value, _, _, err := cn.Get("foo")
	assert.Equalf(t, nil, err, "%v", err)
	assert.Equal(t, "bar", value)

	cn.Del("foo")
}
