package proxy

import (
	"context"
	"github.com/xpwu/go-stream/fakehttp"
)

type Proxy interface {
	Do(ctx context.Context, request *fakehttp.Request) *fakehttp.Response
}

type HeaderFixedKey = string

const (
	MaxResponseKey HeaderFixedKey = "MaxResponseBytes"
)

// 开发者可自行设定 Proxy Creator

var Creator func(conf *ConfigVar) Proxy = NewHttp

//func NewProxy(conf *ConfigVar) Proxy {
//  //compileConf()
//  return NewHttp(conf)
//}
