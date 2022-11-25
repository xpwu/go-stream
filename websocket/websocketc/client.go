package websocketc

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/xpwu/go-log/log"
	"github.com/xpwu/go-reqid/reqid"
	"github.com/xpwu/go-stream/fakehttp"
	"github.com/xpwu/go-xnet/connid"
	"strings"
	"sync"
)

type Client struct {
	host          string
	mu            sync.Mutex
	c             *conn
	ctx           context.Context
	connF         func() error
	m             map[uint32]chan *fakehttp.Response
	push          chan []byte
	id            uint32
	connected     bool
	concurrent chan struct{}
	maxBytes      uint32
}

func non() error {
	return nil
}

/*
HeartBeat_s | FrameTimeout_s | MaxConcurrent | MaxBytes | connect id
   HeartBeat_s: 2 bytes, net order
   FrameTimeout_s: 1 byte  ===0
   MaxConcurrent: 1 byte
   MaxBytes: 4 bytes, net order
   connect id: 8 bytes, net order
*/
func (c *Client) readHandshake(conn *websocket.Conn) (peerConnectionID connid.Id, err error) {
  // read handshake
  _, m, err := conn.ReadMessage()
  if err != nil {
    return 0, err
  }
  if len(m) != 16 {
    return 0, fmt.Errorf("len(handshake) should be 16, actual %d", len(m))
  }
  m = m[3:] // ignore HeartBeat_s  FrameTimeout_s
  c.concurrent = make(chan struct{}, int(m[0]))
  m = m[1:]
  c.maxBytes = binary.BigEndian.Uint32(m[0:])
  m = m[4:]
  return connid.Id(binary.BigEndian.Uint64(m)), nil
}

func (c *Client) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connected {
		return nil
	}

	ctx, logger := log.WithCtx(c.ctx)

	logger.PushPrefix(fmt.Sprintf("connect to %s, ", c.host))
	defer logger.PopPrefix()

	logger.Debug("start")
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, c.host, nil)
	if err != nil {
		logger.Error(err)
		return err
	}

	cid,err := c.readHandshake(conn)
  if err != nil {
    logger.Error(err)
    return err
  }

	c.c = newConn(ctx, conn)
	c.c.id = cid

	logger.Debug("end")

	c.connected = true
	c.connF = non

	c.read()

	return nil
}

func (c *Client) read() {
	_, logger := log.WithCtx(c.ctx)
	go func() {
		for {
			logger.Debug("read ...")
			_, m, err := c.c.c.ReadMessage()
			if err != nil {
				logger.Error(err)
				c.close(err)
				return
			}

			res := fakehttp.NewResponseWithStream(m)
			if res.ReqId() == fakehttp.PushReqId {
				logger.Debug(fmt.Sprintf("receive push data len(%d)", len(res.Data())))
				// 超过buffer的数量，自动丢弃
				// 之所以不使用回调方式，是因为上层收到数据后一样要做goroutine的切换，另外上层如果有同步操作，还会阻塞这里的执行
				select {
				case c.push <- res.Data():
				default:
				}

				continue
			}

			logger.Debug(fmt.Sprintf("receive req data reqid(%d)", res.ReqId()))
			ch, ok := c.popChan(res.ReqId())
			if !ok {
				logger.Warning(fmt.Sprintf("not find request of reqid(%d)", res.ReqId()))
				continue
			}

			ch <- res
			close(ch)
		}
	}()
}

func (c *Client) popChan(id uint32) (ch chan *fakehttp.Response, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch, ok = c.m[id]
	delete(c.m, id)
	<-c.concurrent
	return
}

func (c *Client) addChan(ch chan *fakehttp.Response) uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.id++
	c.m[c.id] = ch
	c.concurrent <- struct{}{}
	return c.id
}

func (c *Client) close(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, v := range c.m {
		v <- nil
		close(v)
	}

	c.m = make(map[uint32]chan *fakehttp.Response)
	c.connected = false
	c.connF = c.connect
	c.c.CloseWith(err)
}

func NewClient(host string) *Client {
	if !strings.HasPrefix(host, "://") {
		host = "ws://" + host
	}

	ret := &Client{
		host: host,
		ctx:  context.Background(),
		m:    make(map[uint32]chan *fakehttp.Response),
		push: make(chan []byte, 20),
		id:   10,
	}

	ret.connF = ret.connect

	return ret
}

func (c *Client) Host() string {
	return c.host
}

func (c *Client) send(ctx context.Context, request *fakehttp.Request) (res *fakehttp.Response, err error) {
	ctx, logger := log.WithCtx(ctx)

	if err = c.connF(); err != nil {
		return
	}

	ch := make(chan *fakehttp.Response, 1)
	id := c.addChan(ch)

	logger.Debug(fmt.Sprintf("fake http reqid(%d)", id))
	request.SetReqId(id)

	data := request.Buffers()
	if len(data) > int(c.maxBytes) {
		return nil, errors.New("request is too large")
	}

	err = c.c.Write(request.Buffers())
	if err != nil {
		return
	}

	select {
	case res = <-ch:
		if res == nil {
			return nil, errors.New("client error")
		}
		return
	case <-ctx.Done():
		err = ctx.Err()
		return
	}
}

type option struct {
	headers map[string]string
}

type Option func(o *option) error

func WithHeader(headers map[string]string) Option {
	return func(o *option) error {
		o.headers = headers
		return nil
	}
}

func (c *Client) ReadPush(ctx context.Context) (d []byte, err error) {
	ctx, logger := log.WithCtx(ctx)
	logger.PushPrefix("read push data, ")
	defer logger.PopPrefix()

	select {
	case d = <-c.push:
		logger.Debug(fmt.Sprintf("return data(len=%d)", len(d)))
		return
	case <-ctx.Done():
		logger.Error(err)
		return nil, ctx.Err()
	}
}

func (c *Client) ReadJsonPush(ctx context.Context, resCanJsonUnmarshal interface{}) (err error) {
	ctx, logger := log.WithCtx(ctx)
	logger.PushPrefix("read json push, ")
	defer logger.PopPrefix()

	logger.Debug("start")
	select {
	case d := <-c.push:
		err = json.Unmarshal(d, resCanJsonUnmarshal)
		if err != nil {
			logger.Error(err)
			return err
		}
		logger.Debug("end")
		return
	case <-ctx.Done():
		logger.Error(err)
		return ctx.Err()
	}
}

func (c *Client) Send(ctx context.Context, api string, body []byte,
	options ...Option) (response []byte, err error) {

	ctx, reqId := reqid.WithCtx(ctx)
	ctx, logger := log.WithCtx(ctx)

	logger.PushPrefix(fmt.Sprintf("send Header-Reqid(%s) to api(%s), ", reqId, api))
	defer logger.PopPrefix()

	logger.Debug("start")

	op := &option{
		headers: make(map[string]string),
	}

	for _, o := range options {
		err = o(op)
		if err != nil {
			logger.Error(err)
			return
		}
	}

	if body == nil {
		body = make([]byte, 0)
	}

	op.headers["api"] = api
	op.headers[reqid.HeaderKey] = reqId

	r := &fakehttp.Request{}
	r.Data = body
	r.Header = op.headers

	res, err := c.send(ctx, r)
	if err != nil {
		logger.Error(err)
		return
	}

	if res.Status() != fakehttp.Success {
		err = errors.New(string(res.Data()))
		logger.Error(err)
		return
	}

	response = res.Data()

	logger.Debug("end")
	return
}

func (c *Client) SendJson(ctx context.Context, api string, reqCanJsonMarshal, resCanJsonUnmarshal interface{},
	options ...Option) (err error) {
	ctx, logger := log.WithCtx(ctx)
	logger.PushPrefix("sendJson. ")
	defer logger.PopPrefix()

	logger.Debug("start")
	req, err := json.Marshal(reqCanJsonMarshal)
	if err != nil {
		logger.Error(err)
		return err
	}

	res, err := c.Send(ctx, api, req, options...)
	if err != nil {
		logger.Error(err)
		return err
	}

	err = json.Unmarshal(res, resCanJsonUnmarshal)
	if err != nil {
		logger.Error(err)
		return err
	}

	return
}
