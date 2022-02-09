package pushc

import (
  "context"
  "errors"
  "fmt"
  "github.com/xpwu/go-log/log"
  "github.com/xpwu/go-stream/push/protocol"
  "github.com/xpwu/go-xnet/xtcp"
  "sync"
  "time"
)

type client struct {
  conn       *xtcp.Conn
  connClosed chan struct{}
  mutex      sync.RWMutex
  addr       string
  ctx        context.Context

  mq       map[uint32]chan *protocol.Response
  sequence uint32
  mqMu     sync.Mutex
}

var (
  timeoutE = errors.New("time out")

  clients = sync.Map{}
)

func sendTo(ctx context.Context, addr string, data []byte, token string, subP byte, timeout time.Duration) (res *protocol.Response, err error) {
  c, _ := clients.LoadOrStore(addr, &client{
    conn:       nil,
    connClosed: nil,
    mutex:      sync.RWMutex{},
    addr:       addr,
    mq:         make(map[uint32]chan *protocol.Response),
    sequence:   0,
    mqMu:       sync.Mutex{},
  })

  return c.(*client).send(ctx, data, token, subP, timeout)
}

func (c *client) send(ctx context.Context, data []byte, token string, subP byte, timeout time.Duration) (res *protocol.Response, err error) {
  timer := time.NewTimer(timeout)
  _, logger := log.WithCtx(c.ctx)

  res, err = c.sendOnce(ctx, data, token, subP, timer)
  if err == timeoutE || err == nil || ctx.Err() != nil {
    return
  }

  // 非超时情况，重试一次。需要重试的原因主要是可能在发送的时候，连接断了
  logger.PushPrefix("try again")
  res, err = c.sendOnce(ctx, data, token, subP, timer)
  timer.Stop()
  return
}

func (c *client) sendOnce(ctx context.Context, data []byte, token string, subP byte, timer *time.Timer) (res *protocol.Response, err error) {
  _, logger := log.WithCtx(c.ctx)
  logger.PushPrefix("send once")

  conn, connClosed, err := c.connect()

  resCh := make(chan *protocol.Response)
  seq := c.addChan(resCh)

  r := protocol.NewRequest(conn)
  r.SetSequence(seq)
  r.Data = data
  r.Token = []byte(token)
  r.SubProtocol = subP

  err = r.Write()
  if err != nil {
    logger.Error(err)
    c.popChan(seq)
    c.close(conn, connClosed)
    return
  }

  select {
  case <-connClosed:
    c.popChan(seq)
    return nil, errors.New("connection closed")
  case res = <-resCh:
    return
  case <-timer.C:
    c.popChan(seq)
    return nil, timeoutE
  case <-ctx.Done():
    return nil, ctx.Err()
  case <-c.ctx.Done():
    return nil, c.ctx.Err()
  }
}

func (c *client) connect() (xConn *xtcp.Conn, connClosed chan struct{}, err error) {

  c.mutex.RLock()
  if c.conn != nil {
    ret := c.conn
    ch := c.connClosed
    c.mutex.RUnlock()
    return ret, ch, nil
  }
  c.mutex.RUnlock()

  c.mutex.Lock()
  defer c.mutex.Unlock()
  // read again
  if c.conn != nil {
    return c.conn, c.connClosed, nil
  }

  ctx, logger := log.WithCtx(context.Background())
  logger.PushPrefix("connect to " + c.addr)

  conn, err := xtcp.Dial(ctx, "tcp", c.addr)
  if err != nil {
    logger.Error(err)
    return nil, nil, err
  }

  c.conn = xtcp.NewConn(ctx, conn)
  c.connClosed = make(chan struct{})

  // 设置ctx  logger
  c.ctx, logger = log.WithCtx(context.Background())
  logger.PushPrefix(fmt.Sprintf("conneted(id:%s) to addr(%s)", c.conn.Id(), c.addr))

  c.read(c.conn, c.connClosed)

  return c.conn, c.connClosed, nil
}

func (c *client) close(old *xtcp.Conn, connClosed chan struct{}) {
  c.mutex.RLock()
  // 已有新的连接时，不做任何操作
  if c.conn == nil || old.Id() != c.conn.Id() {
    c.mutex.RUnlock()
    return
  }

  c.mutex.RUnlock()

  c.mutex.Lock()
  defer c.mutex.Unlock()

  _ = c.conn.Close()
  close(connClosed)

  c.conn = nil
}

func (c *client) popChan(sequence uint32) (r chan *protocol.Response, ok bool) {
  c.mqMu.Lock()
  defer c.mqMu.Unlock()

  r, ok = c.mq[sequence]
  delete(c.mq, sequence)

  return
}

func (c *client) addChan(ch chan *protocol.Response) uint32 {
  c.mqMu.Lock()
  defer c.mqMu.Unlock()
  c.sequence++
  c.mq[c.sequence] = ch

  return c.sequence
}

func (c *client) read(conn *xtcp.Conn, connClosed chan struct{}) {
  _, logger := log.WithCtx(c.ctx)
  logger.PushPrefix("read response from " + conn.Id())
  go func() {
    for {
      logger.Debug("read...")
      r, err := protocol.NewResByConn(conn, time.Time{})
      if err != nil {
        logger.Error(err)
        logger.Info("close connect " + conn.Id())
        c.close(conn, connClosed)
        break
      }

      rc, ok := c.popChan(r.R.GetSequence())
      if !ok {
        logger.Warning(fmt.Sprintf("not find request of reqid(%d)", r.R.GetSequence()))
        continue
      }

      rc <- r
      close(rc)
    }
  }()
}
