package proxy

import (
	"context"
	"github.com/xpwu/go-stream/fakehttp"
)

type HeaderFixedKey = string

const (
	MaxResponseKey HeaderFixedKey = "Stream-Max-Response"
)

// Proxy
// 1、request 中的 header 需要透传给上游服务器
// 2、MaxResponseKey => request.Conn.ServerConfig().MaxBytesPerFrame 需要添加到 header
// 3、config文件中配置的 headers 需要传递给上游服务器
// 4、返回的 response.data 不能大于 request.Conn.ServerConfig().MaxBytesPerFrame
type Proxy interface {
	Do(ctx context.Context, request *fakehttp.Request) *fakehttp.Response
}

// Creator 开发者可自行设定 Proxy Creator
var Creator func(conf *ConfigVar) Proxy = NewHttp
