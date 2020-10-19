package service

import (
	"benet/pkg/codec"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/AsynkronIT/protoactor-go/remote"
)

var (
	defaultTimeout = time.Duration(10) * time.Second
	registry       = &serviceRegistry{
		services: make(map[string]Service),
	}
)

type Service interface {
	ID() int64
	Name() string
	OnStarted()
	OnStopping()
	OnStopped()
	Remote() bool
	Receive(ctx actor.Context)

	ServiceSender

	init(id string, seq int64, s Service, closeCallback func())
	getStats() *Stats
}

type ServiceSender interface {
	Send(methodName string, args ...interface{}) error
	Call(methodName string, args ...interface{}) error
	CallWithTimeout(timeout time.Duration, methodName string, args ...interface{}) error
	IsRunning() bool
	IsStopping() bool
	IsStopped() bool
	Stop()
	GracefulStop()
}

type serviceRegistry struct {
	sync.RWMutex
	seq      int64
	services map[string]Service
}

func (r *serviceRegistry) add(s Service) {
	r.Lock()

	r.seq++
	seq := r.seq

	var name string
	if s.Name() != "" {
		if v, ok := r.services[s.Name()]; ok {
			r.Unlock()
			v.Stop()
			r.Lock()
		}
		name = s.Name()
	} else {
		rv := reflect.ValueOf(s)
		name = reflect.Indirect(rv).Type().Name() + strconv.FormatInt(seq, 10)
	}

	s.init(name, seq, s, func() {
		r.remove(name)
	})

	r.services[name] = s
	r.Unlock()
}

func (r *serviceRegistry) get(name string) Service {
	r.RLock()
	s := r.services[name]
	r.RUnlock()
	return s
}

func (r *serviceRegistry) remove(name string) {
	r.Lock()
	delete(r.services, name)
	r.Unlock()
}

func GetStats() []*Stats {
	registry.Lock()

	var services []Service
	for _, s := range registry.services {
		services = append(services, s)
	}

	registry.Unlock()

	sort.Slice(services, func(i, j int) bool {
		return services[i].ID() > services[j].ID()
	})

	var stats []*Stats
	for _, s := range services {
		stats = append(stats, s.getStats().clone())
	}
	return stats
}

func Start(addr string, opts ...remote.RemotingOption) {
	remote.Start(addr, opts...)
}

func Register(s Service) {
	registry.add(s)
}

func Shutdown() {
	registry.Lock()

	var services []Service
	for _, s := range registry.services {
		services = append(services, s)
	}

	registry.Unlock()

	sort.Slice(services, func(i, j int) bool {
		return services[i].ID() > services[j].ID()
	})

	for _, s := range services {
		s.GracefulStop()
	}
}

func Get(name string) Service {
	return registry.get(name)
}

func CallWithTimeout(timeout time.Duration, pid *actor.PID, methodName string, args ...interface{}) error {
	return call(timeout, pid, methodName, args...)
}

func Call(pid *actor.PID, methodName string, args ...interface{}) error {
	return call(defaultTimeout, pid, methodName, args...)
}

// call 调用指定的函数，等待完成 返回结果
func call(timeout time.Duration, pid *actor.PID, methodName string, args ...interface{}) error {
	// local
	if pid.Address == actor.ProcessRegistry.Address {
		s := registry.get(pid.Id)
		if s == nil {
			return fmt.Errorf("service: %s not registered", s.Name())
		}
		return s.CallWithTimeout(timeout, methodName, args...)
	}

	msg := &RemoteMessage{
		MethodName: methodName,
		Type:       1,
	}
	numArgs := len(args)
	if numArgs > 0 && args[0] != nil {
		b, err := codec.MsgPack.Encode(args[0])
		if err != nil {
			return err
		}
		msg.Data = b
	}

	ctx := actor.EmptyRootContext
	result, err := ctx.RequestFuture(pid, msg, timeout).Result()
	if err != nil {
		return err
	}

	resp, ok := result.(*Response)
	if !ok {
		return errors.New("unknown resp")
	}

	if len(resp.Error) > 0 {
		return errors.New(resp.Error)
	}

	if len(resp.Data) > 0 && numArgs == 2 {
		return codec.MsgPack.Decode(resp.Data, args[1])
	}

	return nil
}

// Send 异步调用指定的函数，不关心调用结果
func Send(pid *actor.PID, methodName string, args ...interface{}) error {
	// local
	if pid.Address == actor.ProcessRegistry.Address {
		s := registry.get(pid.Id)
		if s == nil {
			return fmt.Errorf("service: %s not registered", s.Name())
		}
		return s.Send(methodName, args...)
	}

	msg := &RemoteMessage{
		MethodName: methodName,
	}
	if len(args) > 0 {
		b, err := codec.MsgPack.Encode(args[0])
		if err != nil {
			return err
		}
		msg.Data = b
	}

	ctx := actor.EmptyRootContext
	ctx.Send(pid, msg)
	return nil
}
