package websocketc

import (
  "context"
  "github.com/gorilla/websocket"
  "github.com/xpwu/go-log/log"
  conn2 "github.com/xpwu/go-stream/conn"
  "net"
  "sync"
  "sync/atomic"
  "time"
)

var sequence uint32 = 0

type conn struct {
  c       *websocket.Conn
  mu      chan struct{}
  ctx     context.Context
  cancelF context.CancelFunc
  closed  bool
  once    sync.Once
  id      conn2.Id
}

func newConn(ctx context.Context, c *websocket.Conn) *conn {

  ret := &conn{
    c:  c,
    mu: make(chan struct{}, 1),
    id: conn2.NewId(atomic.AddUint32(&sequence, 1)),
  }
  ret.ctx, ret.cancelF = context.WithCancel(ctx)

  return ret
}

func (c *conn) GetVar(name string) string {
  return ""
}

func (c *conn) Id() conn2.Id {
  return c.id
}

func (c *conn) Write(buffers net.Buffers) error {
  // 只能一个goroutines 访问
  c.mu <- struct{}{}
  defer func() {
    <-c.mu
  }()

  err := c.c.SetWriteDeadline(time.Now().Add(5 * time.Second))
  if err != nil {
    return err
  }

  writer, err := c.c.NextWriter(websocket.BinaryMessage)
  if err != nil {
    return err
  }

  for _, d := range buffers {
    if _, err = writer.Write(d); err != nil {
      return err
    }
  }

  return writer.Close()
}

func (c *conn) CloseWith(err error) {
  c.once.Do(func() {
    _, logger := log.WithCtx(c.ctx)
    c.cancelF()
    if err != nil {
      logger.Error(err)
    }
    logger.Info("close connection")
    _ = c.c.Close()
  })
}

func (c *conn) context() context.Context {
  return c.ctx
}
