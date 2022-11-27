package push

import (
  "context"
  "crypto/md5"
  "encoding/hex"
  "fmt"
  "github.com/xpwu/go-log/log"
  "github.com/xpwu/go-reqid/reqid"
  "github.com/xpwu/go-stream/conn"
  "github.com/xpwu/go-stream/fakehttp"
  "github.com/xpwu/go-stream/push/protocol"
  "github.com/xpwu/go-xnet/xtcp"
  "io"
  "time"
)

var hostId = ""

func Start() {

  m5 := md5.Sum([]byte(reqid.RandomID()))
  hostId = hex.EncodeToString(m5[:])

  conn.RegisterVar("pushtoken", func(conn conn.Conn) string {
    token := protocol.Token{HostId: hostId, ConnId: conn.Id()}
    return token.String()
  })

  for _, s := range configValue.Servers {
    if !s.Net.Listen.On() {
      continue
    }

    // 转换数据
    s.AckTimeout = s.AckTimeout * time.Second

    go runServer(s)
  }
}

func runServer(s *server) {
  logger := log.NewLogger()
  defer func() {
    if r := recover(); r != nil {
      logger.Fatal(r)
      logger.Error(fmt.Sprintf("%s server down! will restart after 5 seconds.", s.Net.Listen.LogString()))
      time.Sleep(5*time.Second)
      go runServer(s)
      logger.Info("server restart!")
    }
  }()

  if s.CloseSubProtocolId == s.DataSubProtocolId {
    panic("push config error. CloseSubProtocolId must not be equal to DataSubProtocolId")
  }

  var handler xtcp.HandlerFun = func(conn *xtcp.Conn) {
    ctx, logger := log.WithCtx(conn.Context())
    ctx, cancelF := context.WithCancel(ctx)

    logger.Debug("new connection")

    // read request
    request := read(ctx, conn)

    // 20 表示一个相对大的数而已，预计不会阻塞chan就ok
    end := make(chan error, 20)

    // 一个比较长的时间
    const largeTime = 2*time.Hour
    const keepalive = 30 * time.Second
    timer := time.NewTimer(largeTime)
    count := 0

    _for:
    for {
      select {
      case r,ok := <-request:
        if !ok {
          cancelF()
          _ = conn.Close()
          break _for
        }
        go processRequest(ctx, s, r, end)
        count++
        if !timer.Stop() {
          <-timer.C
        }
        timer.Reset(largeTime)
      case err := <-end:
        if err != nil {
          logger.Error(err)
          cancelF()
          _ = conn.Close()
          break _for
        }
        count--
        if count == 0 {
          if !timer.Stop() {
            <-timer.C
          }
          // 全部处理完后再保持连接30s
          timer.Reset(keepalive)
        }
      case <-timer.C:
        logger.Info("keepalive timeout, close conn")
        cancelF()
        _ = conn.Close()
        break _for
      }
    }

    // no pipeline
    //keepAlive := 5 * time.Second
    //for {
    //  r := protocol.NewRequest(conn)
    //  err := r.Read(keepAlive)
    //  if err == io.EOF {
    //    logger.Debug("connection closed by peer ")
    //    return
    //  }
    //  if err != nil {
    //    logger.Debug("read error: ", err, ", will close connection")
    //    return
    //  }
    //
    //  // 暂时不使用pipeline的方式处理
    //  if err := processRequest(s, r); err != nil {
    //    logger.Error("processRequest error: ", err)
    //    return
    //  }
    //
    //  keepAlive = 30 * time.Second
    //}
  }

  tcpServer := &xtcp.Server{
    Net:     s.Net,
    Handler: handler,
    Name:    "push",
  }

  tcpServer.ServeAndBlock()

}

func read(ctx context.Context, conn *xtcp.Conn) <-chan *protocol.Request {
  _,logger := log.WithCtx(ctx)
  request := make(chan *protocol.Request)
  go func() {
    defer func() {
      close(request)
    }()
    for {
      r := protocol.NewRequest(conn)
      err := r.Read(time.Time{})
      if err == io.EOF {
        logger.Debug("connection closed by peer ")
        return
      }
      if err != nil {
        logger.Debug("read error: ", err, ", will close connection")
        return
      }
      request <- r
    }
  }()
  return request
}

func processRequest(ctx context.Context, s *server, r *protocol.Request, end chan error) {
  ctx, logger := log.WithCtx(ctx)

  token, err := protocol.ResumeToken(r.Token, hostId)
  if err != nil {
    logger.Error(err)
    end <- protocol.NewResponse(r, protocol.HostNameErr).Write()
  }

  con, ok := conn.GetConn(token.ConnId)
  if !ok {
    logger.Error(fmt.Sprintf("can not find conn with id(%v)", token))
    end <- protocol.NewResponse(r, protocol.TokenNotExist).Write()
    return
  }

  if r.SubProtocol == s.CloseSubProtocolId {
    logger.Info(fmt.Sprintf("close subprotocol. will close conn(id=%s). ", con.Id()))
    con.CloseWith(nil)
    end <- protocol.NewResponse(r).Write()
    return
  }

  // 写给客户端
  logger.Debug(fmt.Sprintf("push data(len=%d) to client connection(conn_id=%s)", len(r.Data), con.Id()))
  pushId := fakehttp.GetPushID(con)
  // 先准备好ack的接收
  ch := pushId.WaitAck()
  clientRes := fakehttp.NewResponseWithPush(con, pushId.ToBytes(), r.Data)
  if err = clientRes.Write(); err != nil {
    logger.Error(fmt.Sprintf("write push data to client conn(id=%s) err. ", con.Id()), err)
    con.CloseWith(fmt.Errorf("write push data from push conn(id=%s) err, %v. Will close this connection(id=%s). ",
      r.Conn.Id(), err, con.Id()))

    end <- protocol.NewResponse(r, protocol.ServerInternalErr).Write()
    return
  }

  // 根据配置要求，0 表示不等待ack
  if s.AckTimeout == 0 {
    end <- protocol.NewResponse(r).Write()
    return
  }

  // 写上游服务器

  timer := time.NewTimer(s.AckTimeout)

  select {
  case _,ok := <- ch:
    if !timer.Stop() {
      <-timer.C
    }
    if !ok {
      end <- protocol.NewResponse(r, protocol.ServerInternalErr).Write()
    } else {
      end <- protocol.NewResponse(r).Write()
    }
  case <- timer.C:
    pushId.CancelWaitingAck()
    logger.Warning("wait client ack timeout")
    end <- protocol.NewResponse(r, protocol.Timeout).Write()
  case <-ctx.Done():
    end <- protocol.NewResponse(r, protocol.ServerInternalErr).Write()
  }

}
