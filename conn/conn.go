package conn

import (
	"fmt"
	"github.com/xpwu/go-var/vari"
	"net"
	"strconv"
	"sync"
	"time"
)

type Id uint64

func (id Id) String() string {
	return fmt.Sprintf("%x", uint64(id))
}

func ResumeIdFrom(str string) (id Id, err error) {
	i, err := strconv.ParseUint(str, 16, 64)
	if err != nil {
		return
	}

	id = Id(i)
	return
}

// 用过即废，尽最大可能不重复id，防止窜连接
func NewId(sequence uint32) Id {
	t := uint64(time.Now().Unix())
	return Id((t << 32) + uint64(sequence))
}

type ExtraValue interface {
	UnInit()
}

// 并发安全
type extraValueInterface interface {
	Load(interface{}) (ExtraValue, bool)
	Store(key interface{}, value ExtraValue)
	LoadOrStore(key interface{}, value ExtraValue) (actual ExtraValue, loaded bool)
	LoadAndDelete(key interface{}) (value ExtraValue, loaded bool)
	Delete(interface{})
	Range(func(key interface{}, value ExtraValue) (shouldContinue bool))
}

type Base sync.Map

func (b *Base) Load(i interface{}) (ExtraValue, bool) {
	v,ok := ((*sync.Map)(b)).Load(i)
	return v.(ExtraValue), ok
}

func (b *Base) Store(key interface{}, value ExtraValue) {
	((*sync.Map)(b)).Store(key, value)
}

func (b *Base) LoadOrStore(key interface{}, value ExtraValue) (actual ExtraValue, loaded bool) {
	v,ok := ((*sync.Map)(b)).LoadOrStore(key, value)
	return v.(ExtraValue), ok
}

func (b *Base) LoadAndDelete(key interface{}) (value ExtraValue, loaded bool) {
	v,ok := ((*sync.Map)(b)).LoadAndDelete(key)
	return v.(ExtraValue), ok
}

func (b *Base) Delete(i interface{}) {
	((*sync.Map)(b)).Delete(i)
}

func (b *Base) Range(f func(key interface{}, value ExtraValue) (shouldContinue bool)) {
	f1 := func(key interface{}, value interface{}) (shouldContinue bool) {
		return f(key, value.(ExtraValue))
	}
	((*sync.Map)(b)).Range(f1)
}

func (b *Base) Close() {
	b.Range(func(key interface{}, value ExtraValue) (shouldContinue bool) {
		value.UnInit()
		return true
	})
}

type Conn interface {
	extraValueInterface
	vari.VarObject
	Id() Id
	// 所有的实现中，需要满足 multiple goroutines 的同时调用
	Write(buffers net.Buffers) error

	// 所有的写将中断，并返回错误
	CloseWith(err error)
}

var (
	connMap = &sync.Map{}
)

func AddConn(conn Conn) {
	connMap.Store(conn.Id(), conn)
}

func DelConn(conn Conn) {
	connMap.Delete(conn.Id())
}

func GetConn(id Id) (conn Conn, ok bool) {
	res, ok := connMap.Load(id)
	if ok {
		conn = res.(Conn)
	}

	return
}

var (
	varMap = make(map[string]func(conn Conn) string)
	mu     sync.RWMutex
)

// 静态注册，不能在服务过程中再注册
func RegisterVar(name string, value func(conn Conn) string) {
	mu.Lock()
	varMap[name] = value
	mu.Unlock()
}

func GetVarValue(name string, conn Conn) (value string, ok bool) {
	mu.RLock()
	f, ok := varMap[name]
	mu.RUnlock()

	if !ok {
		return
	}

	return f(conn), ok
}
