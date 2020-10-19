package service

import (
	"benet/pkg/codec"
	"benet/pkg/helpers"
	"benet/pkg/log"
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/AsynkronIT/protoactor-go/mailbox"
	"github.com/AsynkronIT/protoactor-go/scheduler"
)

const (
	StateStopped = iota
	StateStopping
	StateRunning
)

type Handler struct {
	method   reflect.Method
	argTypes []reflect.Type
	numArgs  int
}

type timerMessage struct {
	task   func()
	repeat bool
}

type Message struct {
	method reflect.Method
	args   []reflect.Value
	done   chan struct{}
	err    error
}

type Stats struct {
	sync.RWMutex

	Name         string
	TotalMsgs    int64
	Messages     int64
	TimeoutCount int64
	TimeoutLogs  []string
	SlowCount    int64
	SlowLogs     []string
}

func (stats *Stats) MailboxStarted() {}
func (stats *Stats) MailboxEmpty()   {}
func (stats *Stats) MessagePosted(msg interface{}) {
	atomic.AddInt64(&stats.Messages, 1)
	atomic.AddInt64(&stats.TotalMsgs, 1)
}
func (stats *Stats) MessageReceived(msg interface{}) {
	atomic.AddInt64(&stats.Messages, -1)
}

func (stats *Stats) addTimeout(method string) {
	atomic.AddInt64(&stats.TimeoutCount, 1)

	stats.Lock()
	defer stats.Unlock()
	stats.TimeoutLogs = append(stats.TimeoutLogs, method)
	if len(stats.TimeoutLogs) > 30 {
		stats.TimeoutLogs = stats.TimeoutLogs[1:]
	}
}

func (stats *Stats) addSlow(method string) {
	atomic.AddInt64(&stats.SlowCount, 1)

	stats.Lock()
	defer stats.Unlock()

	stats.SlowLogs = append(stats.SlowLogs, method)
	if len(stats.SlowLogs) > 30 {
		stats.SlowLogs = stats.SlowLogs[1:]
	}
}

func (stats *Stats) clone() *Stats {
	s := &Stats{
		Name:         stats.Name,
		TotalMsgs:    atomic.LoadInt64(&stats.TotalMsgs),
		Messages:     atomic.LoadInt64(&stats.Messages),
		TimeoutCount: atomic.LoadInt64(&stats.TimeoutCount),
		SlowCount:    atomic.LoadInt64(&stats.SlowCount),
	}

	s.RLock()
	defer s.RUnlock()

	for _, v := range stats.TimeoutLogs {
		s.TimeoutLogs = append(s.TimeoutLogs, v)
	}
	for _, v := range stats.SlowLogs {
		s.SlowLogs = append(s.SlowLogs, v)
	}

	return s
}

type Base struct {
	id             int64
	pid            *actor.PID
	name           string
	service        Service
	handlers       map[string]*Handler
	parent         reflect.Value
	state          int32
	timerScheduler *scheduler.TimerScheduler
	stopCallbacks  []func()
	stats          *Stats
}

func (base *Base) Name() string { return "" }
func (base *Base) OnStarted()   {}
func (base *Base) OnStopping()  {}
func (base *Base) OnStopped()   {}
func (base *Base) Remote() bool { return false }

// Stop 立刻停止，忽略未处理的消息
func (base *Base) Stop() {
	base.stop(false)
}

// GracefulStop 等待所有消息处理完成后停止
func (base *Base) GracefulStop() {
	base.stop(true)
}

func (base *Base) stop(graceful bool) {
	if !base.IsRunning() {
		return
	}

	log.Infof("service(%s): stopping", base.name)
	atomic.StoreInt32(&base.state, StateStopping)
	base.service.OnStopping()
	if base.pid != nil {
		if graceful {
			base.pid.GracefulPoison()
		} else {
			base.pid.Stop()
		}
	}

	for _, cb := range base.stopCallbacks {
		cb()
	}
}

func (base *Base) IsRunning() bool {
	return atomic.LoadInt32(&base.state) == StateRunning
}

func (base *Base) IsStopping() bool {
	return atomic.LoadInt32(&base.state) == StateStopping
}

func (base *Base) IsStopped() bool {
	return atomic.LoadInt32(&base.state) == StateStopped
}

// Receive implements the actor interface
func (base *Base) Receive(ctx actor.Context) {
	defer helpers.Recover()

	switch msg := ctx.Message().(type) {
	case *actor.Started:
		log.Infof("service(%s): started", base.name)
		base.service.OnStarted()
	case *actor.Stopped:
		log.Infof("service(%s): stopped", base.name)
		atomic.StoreInt32(&base.state, StateStopped)
		base.service.OnStopped()
	case *Message:
		base.handleLocalMessage(msg)
	case *RemoteMessage:
		if !base.service.Remote() {
			log.Errorf("service(%s): invalid msg", base.name)
			return
		}

		result, err := base.handleRemoteMessage(msg)
		if msg.Type == 1 {
			resp := &Response{}
			if err != nil {
				resp.Error = err.Error()
			} else {
				resp.Data = result
			}
			ctx.Respond(resp)
		}
	case *timerMessage:
		msg.task()
	}
}

func (base *Base) init(name string, id int64, s Service, cb func()) {
	base.service = s
	base.id = id
	base.stats = &Stats{Name: name}

	ctx := actor.EmptyRootContext
	props := actor.PropsFromProducer(func() actor.Actor { return s })
	props.WithMailbox(mailbox.Unbounded(base.stats))
	pid, err := ctx.SpawnNamed(props, name)
	if err != nil {
		panic(err)
	}

	base.pid = pid
	base.state = StateRunning
	base.addStopCallback(cb)

	if len(base.name) == 0 {
		base.parent = reflect.ValueOf(s)
		base.name = name
		base.handlers = suitableMethods(reflect.TypeOf(s))
	}
}

func (s *Base) getStats() *Stats {
	return s.stats
}

func (s *Base) ID() int64 {
	return s.id
}

func (s *Base) addStopCallback(cb func()) {
	s.stopCallbacks = append(s.stopCallbacks, cb)
}

func (s *Base) RegisterHandler(h interface{}) {
	handlers := suitableMethods(reflect.TypeOf(h))
	for k, v := range handlers {
		s.handlers[k] = v
	}
}

func suitableMethods(rt reflect.Type) map[string]*Handler {
	handlers := make(map[string]*Handler)
	for m := 0; m < rt.NumMethod(); m++ {
		method := rt.Method(m)
		mtype := method.Type

		if method.PkgPath != "" {
			continue
		}

		var argTypes []reflect.Type
		for i := 1; i < mtype.NumIn(); i++ {
			argTypes = append(argTypes, mtype.In(i))
		}

		handlers[method.Name] = &Handler{
			method:   method,
			argTypes: argTypes,
			numArgs:  len(argTypes),
		}
	}

	return handlers
}

func (base *Base) getHandler(name string) (*Handler, bool) {
	v, ok := base.handlers[name]
	return v, ok
}

func (base *Base) newMessage(methodName string, isCall bool, args ...interface{}) (*Message, error) {
	handler, ok := base.getHandler(methodName)
	if !ok {
		return nil, fmt.Errorf("service(%s): %s not defined", base.name, methodName)
	}

	numArgs := len(args)
	if numArgs != handler.numArgs {
		return nil, fmt.Errorf("service(%s): method: %s wrong number of arguments", base.name, methodName)
	}

	values := make([]reflect.Value, handler.numArgs+1)
	values[0] = base.parent
	for k, v := range args {
		values[k+1] = reflect.ValueOf(v)
	}

	msg := &Message{
		method: handler.method,
		args:   values,
	}
	if isCall {
		msg.done = make(chan struct{}, 1)
	}

	return msg, nil
}

// SendMsg 发送异步消息
func (base *Base) SendMsg(msg interface{}) error {
	if !base.IsRunning() {
		return fmt.Errorf("service(%s): service is not running", base.name)
	}
	base.pid.Tell(msg)
	return nil
}

// CallWithTimeout 调用指定的服务方法，等待完成。指定的timeout未完成将返回err
func (base *Base) CallWithTimeout(timeout time.Duration, methodName string, args ...interface{}) error {
	if !base.IsRunning() {
		return fmt.Errorf("service(%s): service is not running", base.name)
	}

	msg, err := base.newMessage(methodName, true, args...)
	if err != nil {
		return err
	}

	if err := base.SendMsg(msg); err != nil {
		return err
	}

	now := time.Now()
	ctx, cancel := context.WithDeadline(context.Background(), now.Add(timeout))
	select {
	case <-ctx.Done():
		base.stats.addTimeout(methodName)
		return fmt.Errorf("service(%s): method: %s call timeout", base.name, methodName)
	case <-msg.done:
		cancel()
	}

	return msg.err
}

// Call 调用指定的函数，等待完成 返回结果
func (base *Base) Call(methodName string, args ...interface{}) error {
	return base.CallWithTimeout(time.Second*5, methodName, args...)
}

// Send 异步调用指定的函数，不关心调用结果
func (base *Base) Send(methodName string, args ...interface{}) error {
	if !base.IsRunning() {
		return fmt.Errorf("service(%s): service is not running", base.name)
	}

	msg, err := base.newMessage(methodName, false, args...)
	if err != nil {
		return err
	}

	return base.SendMsg(msg)
}

// AfterFunc 指定的时间到期后调用
func (base *Base) AfterFunc(d time.Duration, repeat bool, fn func()) scheduler.CancelFunc {
	if base.timerScheduler == nil {
		base.timerScheduler = scheduler.NewTimerScheduler()
	}

	msg := &timerMessage{task: fn}
	var cancel scheduler.CancelFunc
	if repeat {
		cancel = base.timerScheduler.SendRepeatedly(d, d, base.pid, msg)
		base.addStopCallback(cancel)
	} else {
		cancel = base.timerScheduler.SendOnce(d, base.pid, msg)
	}

	return cancel
}

func (base *Base) handleLocalMessage(msg *Message) {
	f := msg.method.Func
	if result := f.Call(msg.args); len(result) > 0 {
		errInter := result[0].Interface()
		if errInter != nil {
			msg.err = errInter.(error)
		}
	}

	if msg.done != nil {
		msg.done <- struct{}{}
	}
}

func (base *Base) handleRemoteMessage(msg *RemoteMessage) ([]byte, error) {
	handler, ok := base.getHandler(msg.MethodName)
	if !ok {
		return nil, fmt.Errorf("service(%s): %s not defined", base.name, msg.MethodName)
	}

	var idx int
	var args []reflect.Value
	args = append(args, base.parent)
	arg1 := len(msg.Data) == 0

	// Call(pid, "methodName", msg)    method(msg string)
	if !arg1 && handler.numArgs > 0 {
		rt := handler.argTypes[0]
		v := getPtrValue(rt)
		if err := codec.MsgPack.Decode(msg.Data, v.Interface()); err != nil {
			return nil, err
		}
		if rt.Kind() == reflect.Ptr {
			args = append(args, v)
		} else {
			args = append(args, v.Elem())
		}
		idx++
	}

	// Call(pid, "methodName", nil, &reply)    method(reply *string)
	// Call(pid, "methodName", msg, &reply)    method(msg string, reply *string)
	var reply interface{}
	if arg1 && handler.numArgs == 1 || !arg1 && handler.numArgs == 2 {
		v := getPtrValue(handler.argTypes[idx])
		reply = v.Interface()
		args = append(args, v)
	}

	f := handler.method.Func
	if result := f.Call(args); len(result) > 0 {
		errInter := result[0].Interface()
		if errInter != nil {
			return nil, errInter.(error)
		}
	}

	if reply == nil {
		return nil, nil
	}

	return codec.MsgPack.Encode(reply)
}

func getPtrValue(rt reflect.Type) (rv reflect.Value) {
	if rt.Kind() == reflect.Ptr {
		rv = reflect.New(rt.Elem())
	} else {
		rv = reflect.New(rt)
	}
	return
}
