package proxy

import (
	"context"
	"errors"
	"fmt"
	"github.com/xpwu/go-httpclient/httpc"
	"github.com/xpwu/go-log/log"
	"github.com/xpwu/go-stream/fakehttp"
	"io/ioutil"
	"net/http"
	"net/textproto"
	"strconv"
)

type httpP struct {
	conf *ConfigVar
}

func NewHttp(conf *ConfigVar) Proxy {
	return &httpP{conf: conf}
}

func (h *httpP) Do(ctx context.Context, request *fakehttp.Request) (fres *fakehttp.Response) {
	ctx, logger := log.WithCtx(ctx)

	defer func() {
		if r := recover(); r != nil {
			logger.Fatal(r)
			fres = fakehttp.NewResponseWithFailed(request, errors.New("stream internal error"))
		}
	}()

	url := httpc.RawURL(h.conf.Url.ValueWith(request)).String()

	logger.PushPrefix(fmt.Sprintf("proxy to %s,", url))

	headers := make(http.Header)

	for key, value := range request.Header {
		key = textproto.CanonicalMIMEHeaderKey(key)
		logger.Debug(fmt.Sprintf("add header: %s=>%s", key, value))
		headers.Add(key, value)
	}

	MaxResponseValue := strconv.Itoa(int(request.Conn.ServerConfig().MaxBytesPerFrame))
	headers.Add(MaxResponseKey, MaxResponseValue)
	logger.Debug(fmt.Sprintf("add header: %s=>%s", MaxResponseKey, MaxResponseValue))

	for _, header := range h.conf.Headers {
		key := textproto.CanonicalMIMEHeaderKey(header.Key)
		value := header.Value.ValueWith(request)
		logger.Debug(fmt.Sprintf("add header: %s=>%s", key, value))
		headers.Add(key, value)
	}

	var response *http.Response
	err := httpc.Send(ctx, url, httpc.WithHeader(headers),
		httpc.WithBytesBody(request.Data), httpc.WithResponse(&response))
	if err != nil {
		logger.Error(err)
		fres = fakehttp.NewResponseWithFailed(request, errors.New("500 Stream-Internal Server Error"))
		return
	}

	if response.StatusCode != http.StatusOK {
		logger.Error(response.Status)
		fres = fakehttp.NewResponseWithFailed(request, errors.New(response.Status))
		return
	}

	defer func() {
		_ = response.Body.Close()
	}()

	if response.ContentLength > int64(request.Conn.ServerConfig().MaxBytesPerFrame) {
		logger.Error(fmt.Sprintf("response(size=%d) is too large!", response.ContentLength))
		fres = fakehttp.NewResponseWithFailed(request, errors.New("response is too large"))
		return
	}

	res, err := ioutil.ReadAll(response.Body)
	if err != nil {
		logger.Error(err)
		fres = fakehttp.NewResponseWithFailed(request, errors.New("500 Stream-Internal Server Error"))
		return
	}

	if uint64(len(res)) > uint64(request.Conn.ServerConfig().MaxBytesPerFrame) {
		logger.Error(fmt.Sprintf("response(size=%d) is too large!", uint64(len(res))))
		fres = fakehttp.NewResponseWithFailed(request, errors.New("response is too large"))
		return
	}

	logger.Debug("end")
	return fakehttp.NewResponseWithSuccess(request, res)
}
