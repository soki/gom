package service

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var num int32

type Hello struct {
	Base

	name string
	n    int
}

func (h *Hello) Name() string { return h.name }

func (h *Hello) World(s *string) {
	*s = "World"
}

func (h *Hello) Inc(n int) {
	if h.n == 20 {
		return
	}
	h.n += n
}

func (h *Hello) GetN(n *int) {
	*n = h.n
}

func (h *Hello) Timeout() {
	time.Sleep(time.Second)
}

func (h *Hello) Inc2(n int) {
	time.Sleep(time.Millisecond * 50)
	h.n += n
	atomic.StoreInt32(&num, int32(h.n))
}

func TestRegister(t *testing.T) {
	Register(&Hello{name: "Hello"})

	a := Get("Hello")
	assert.NotNil(t, a)

	b := Get("Hello2")
	assert.Nil(t, b)

	a.Stop()
	c := Get("Hello")
	assert.Nil(t, c)
}

func TestCall(t *testing.T) {
	s := &Hello{name: "A"}
	Register(s)

	var w string
	s.Call("World", &w)
	assert.Equal(t, w, "World")
}

func TestCallWithTimeout(t *testing.T) {
	s := &Hello{name: "B"}
	Register(s)
	err := s.CallWithTimeout(time.Millisecond*100, "Timeout")
	errmsg := "service: Hello.Timeout call timeout"
	assert.EqualErrorf(t, err, errmsg, "")
}

func TestSend(t *testing.T) {
	s := &Hello{name: "C"}
	Register(s)

	s.Send("Inc", 1)
	var n int
	s.Call("GetN", &n)
	assert.Equal(t, n, 1)
}

func TestAfterFunc(t *testing.T) {
	s := &Hello{name: "D"}
	Register(s)

	s.AfterFunc(time.Millisecond, false, func() {
		s.Inc(1)
	})

	time.Sleep(time.Second)
	var n int
	s.Call("GetN", &n)
	assert.Equal(t, n, 1)

	s.AfterFunc(time.Millisecond, true, func() {
		s.Inc(1)
	})

	time.Sleep(time.Second)
	s.Call("GetN", &n)
	assert.Equal(t, n, 20)
}

func TestGracefulStop(t *testing.T) {
	s := &Hello{name: "E"}
	Register(s)

	for i := 0; i < 10; i++ {
		s.Send("Inc2", 1)
	}

	s.GracefulStop()
	n := atomic.LoadInt32(&num)
	assert.Equal(t, n, int32(10))
}
