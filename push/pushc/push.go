package pushc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/xpwu/go-log/log"
	"github.com/xpwu/go-stream/push/protocol"
	"github.com/xpwu/go-xnet/xtcp"
	"strings"
	"time"
)

const (
	dataSubProtocolId = 0
	closeSubProtocol  = 1
)

func subProtocolText(sub byte) string {
	switch sub {
	case dataSubProtocolId:
		return "dataSubProtocolId"
	case closeSubProtocol:
		return "closeSubProtocol"
	default:
		return ""
	}
}

// host:port/pushToken
func send(ctx context.Context, pushUrl string, sub byte, data []byte) (err error) {
	ctx, logger := log.WithCtx(ctx)

	urls := strings.Split(pushUrl, "/")
	if len(urls) != 2 {
		err = fmt.Errorf("pushurl(%s) error", pushUrl)
		logger.Error(err)
		return
	}

	if len(urls[1]) != protocol.TokenLen {
		err = fmt.Errorf("len token(%s) error", urls[1])
		logger.Error(err)
		return
	}

	logger.Debug("token:" + urls[1])

	// 连接的ctx 与 单次请求的 ctx 不应是同一个
	cctx := context.Background()

	c, err := xtcp.Dial(cctx, "tcp", urls[0])
	if err != nil {
		logger.Error(err)
		return
	}
	defer func() {
		_ = c.Close()
	}()

	tcpC := xtcp.NewConn(cctx, c)
	defer func() {
		_ = tcpC.Close()
	}()

	r := protocol.NewRequest(tcpC)
	r.SetSequence(1)
	r.Data = data
	r.Token = []byte(urls[1])
	r.SubProtocol = sub

	err = r.Write()
	if err != nil {
		logger.Error(err)
		return
	}

	res, err := protocol.NewResByConn(tcpC, 5*time.Second)
	if err != nil {
		logger.Error(err)
		return
	}

	if res.R.GetSequence() != r.GetSequence() {
		err = errors.New("sequence of request and response are not equal. ")
		logger.Error(err)
		return
	}

	res.R = r
	if res.State != protocol.Success {
		err = errors.New(protocol.StateText(res.State))
		logger.Error(err)
		return
	}

	return nil
}

func Close(ctx context.Context, pushUrl string) error {
	ctx, logger := log.WithCtx(ctx)

	logger.PushPrefix(fmt.Sprintf("push to %s for close that collection, ", pushUrl))
	defer logger.PopPrefix()

	logger.Debug("start. ")
	err := send(ctx, pushUrl, closeSubProtocol, make([]byte, 0))
	if err != nil {
		logger.Error("error, ", err)
		return err
	}
	logger.Debug("end. ")
	return nil
}

func PushData(ctx context.Context, pushUrl string, data []byte) error {
	ctx, logger := log.WithCtx(ctx)

	logger.PushPrefix(fmt.Sprintf("push data(len=%d) to %s, ", len(data), pushUrl))
	defer logger.PopPrefix()

	logger.Debug("start. ")
	err := send(ctx, pushUrl, dataSubProtocolId, data)
	if err != nil {
		logger.Error("error, ", err)
		return err
	}
	logger.Debug("end. ")
	return nil
}

func PushJsonData(ctx context.Context, pushUrl string, st interface{}) error {
	d, err := json.Marshal(st)
	if err != nil {
		return err
	}

	return PushData(ctx, pushUrl, d)
}
