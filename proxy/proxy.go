package proxy

import (
  "context"
  "github.com/xpwu/go-stream/fakehttp"
)

type Proxy interface {
  Do(ctx context.Context, request *fakehttp.Request) *fakehttp.Response
}

func NewProxy(conf *ConfigVar) Proxy {
  //compileConf()
  return newHttp(conf)
}
