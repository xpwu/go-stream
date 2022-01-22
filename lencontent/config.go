package lencontent

import (
  "fmt"
  "github.com/xpwu/go-config/configs"
  "github.com/xpwu/go-log/log"
  "github.com/xpwu/go-stream/proxy"
  "github.com/xpwu/go-xnet/xtcp"
  "time"
)

type config struct {
  Servers []*server
}

type server struct {
  Net                        *xtcp.Net
  HeartBeat_s                time.Duration
  Proxy                      *proxy.Config
  ProxyVar                   *proxy.ConfigVar `json:"-"`
  MaxConcurrentPerConnection int              `json:"-"`
  FrameTimeout_s             time.Duration    `json:"-"`
  MaxBytesPerFrame           uint32              `json:"-"`
}

func (s *server) checkValue(logger *log.Logger) {
  logger.PushPrefix(fmt.Sprintf("listen(%s)", s.Net.Listen.LogString()))

  if s.MaxConcurrentPerConnection > 20 {
    logger.Warning("MaxConcurrentPerConnection must be less than 20! Use default value: 5")
  }
  if s.MaxConcurrentPerConnection <= 0 || s.MaxConcurrentPerConnection > 20 {
    s.MaxConcurrentPerConnection = 5
  }

  s.ProxyVar = proxy.CompileConf(s.Proxy)

  if s.HeartBeat_s > 65535 {
    logger.Warning("HeartBeat_s must be less than 65535! Use default value: 240")
  }
  if s.HeartBeat_s < 1 || s.HeartBeat_s > 65535 {
    s.HeartBeat_s = 4 * 60
  }
  s.HeartBeat_s *= time.Second

  if s.FrameTimeout_s > 255 {
    logger.Warning("FrameTimeout_s must be less than 255! Use default value: 10")
  }
  if s.FrameTimeout_s > 255 || s.FrameTimeout_s <= 0 {
    s.FrameTimeout_s = 10
  }
  s.FrameTimeout_s *= time.Second
}

var configValue = &config{
  Servers: []*server{
    {
      Net:                        xtcp.DefaultNetConfig(),
      HeartBeat_s:                4 * 60,
      Proxy:                      proxy.DefaultConfig(),
      MaxConcurrentPerConnection: 5,
      FrameTimeout_s:             10,
      MaxBytesPerFrame:           1 * 1024 * 1024,
    },
  },
}

func init() {
  configs.Unmarshal(configValue)
}
