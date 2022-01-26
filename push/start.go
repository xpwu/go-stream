package push

import (
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
    token := protocol.Token{HostId: hostId, ConnId: conn.Id().String()}
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
    _, logger = log.WithCtx(conn.Context())
    logger.PushPrefix(fmt.Sprintf("push(conn_id=%s)", conn.Id()))
    logger.Debug(fmt.Sprintf("new connection from %s", conn.RemoteAddr()))

    keepAlive := 5 * time.Second
    for {
      r := protocol.NewRequest(conn)
      err := r.Read(keepAlive)
      if err == io.EOF {
        logger.Debug("connection closed by peer ")
        return
      }
      if err != nil {
        logger.Debug("read error: ", err, ", will close connection")
        return
      }

      // 暂时不使用pipeline的方式处理
      if err := processRequest(s, r); err != nil {
        logger.Error("processRequest error: ", err)
        return
      }

      keepAlive = 30 * time.Second
    }
  }

  tcpServer := &xtcp.Server{
    Net:     s.Net,
    Handler: handler,
    Name:    "push",
  }

  tcpServer.ServeAndBlock()

}

func processRequest(s *server, r *protocol.Request) error {
  _, logger := log.WithCtx(r.Conn.Context())

  token, err := protocol.ResumeToken(r.Token, hostId)
  if err != nil {
    logger.Error(err)
    return protocol.NewResponse(r, protocol.HostNameErr).Write()
  }
  cid, err := conn.ResumeIdFrom(token.ConnId)
  if err != nil {
    logger.Error(err)
    return protocol.NewResponse(r, protocol.TokenNotExist).Write()
  }

  con, ok := conn.GetConn(cid)
  if !ok {
    logger.Error(fmt.Sprintf("can not find conn with id(%v)", token))
    return protocol.NewResponse(r, protocol.TokenNotExist).Write()
  }

  if r.SubProtocol == s.CloseSubProtocolId {
    logger.Debug(fmt.Sprintf("close subprotocol. will close conn(id=%s)", con.Id()))
    con.CloseWith(fmt.Errorf("close subprotocol. will close conn(id=%s)", con.Id()))
    return protocol.NewResponse(r).Write()
  }

  // 写给客户端
  logger.Debug(fmt.Sprintf("push data(len=%d) to client connection(conn_id=%s)", len(r.Data), con.Id()))
  pushId := fakehttp.GetPushID(con)
  // 先准备好ack的接收
  ch := pushId.WaitAck()
  clientRes := fakehttp.NewResponseWithPush(con, pushId.ToBytes(), r.Data)
  if err = clientRes.Write(); err != nil {
    logger.Error(fmt.Sprintf("write push data to client conn(id=%s) err. ", con.Id()), err)
    con.CloseWith(fmt.Errorf("write push data from push conn(id=%s) err, %v. Will close this connection(id=%s)",
      r.Conn.Id(), err, con.Id()))

    return protocol.NewResponse(r, protocol.ServerInternalErr).Write()
  }

  // 根据配置要求，0 表示不等待ack
  if s.AckTimeout == 0 {
    return protocol.NewResponse(r).Write()
  }

  // 写上游服务器
  // todo 上游服务器已断开连接的情况下，这里仍会等待

  timer := time.NewTimer(s.AckTimeout)

  select {
  case _,ok := <- ch:
    timer.Stop()
    if !ok {
      return protocol.NewResponse(r, protocol.ServerInternalErr).Write()
    }
  case <- timer.C:
    pushId.CancelWaitingAck()
    logger.Warning("wait client ack timeout")
    return protocol.NewResponse(r, protocol.Timeout).Write()
  }

  return protocol.NewResponse(r).Write()
}
