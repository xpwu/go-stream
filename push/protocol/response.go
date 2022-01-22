package protocol

import (
	"github.com/xpwu/go-log/log"
	"github.com/xpwu/go-xnet/xtcp"
	"io"
	"time"
)

/**
Request:
   sequence | token | subprotocol | len | <data>
     sizeof(sequence) = 4. net order
     sizeof(token) = 32 . hex
     sizeof(subprotocol) = 1.
     sizeof(len) = 4. len = sizof(data) net order
     data: subprotocol Request data

  Response:
   sequence | state | len | <data>
     sizeof(sequence) = 4. net order
     sizeof(state) = 1.
               state = 0: success; 1: hostname error
                ; 2: token not exist; 3: server intelnal error
     sizeof(len) = 4. len = sizeof(data) net order
     data: subprotocol Response data
*/

type state byte

const (
	Success state = iota
	HostNameErr
	TokenNotExist
	ServerInternalErr
)

func StateText(s state) string {
	switch s {
	case Success:
		return "Success"
	case HostNameErr:
		return "HostNameErr"
	case TokenNotExist:
		return "TokenNotExist"
	case ServerInternalErr:
		return "ServerInternalErr"
	default:
		return "Unknown"
	}
}

type Response struct {
	R     *Request
	State state
	// Data     []byte  暂时无需回传数据
}

func NewResponse(request *Request, st ...state) *Response {
	res := &Response{
		R:     request,
		State: Success,
	}

	if len(st) == 1 {
		res.State = st[0]
	}

	return res
}

func (r *Response) Write() error {
	buffers := make([][]byte, 2)
	buffers[0] = r.R.Sequence
	st := make([]byte, 1+4)
	st[0] = byte(r.State)
	buffers[1] = st

	_, err := r.R.Conn.WriteBuffers(buffers)
	return err
}

func NewResByConn(c *xtcp.Conn, d time.Duration) (res *Response, err error) {
	_, logger := log.WithCtx(c.Context())

	res = &Response{
		R: &Request{
			Conn:     c,
			Sequence: make([]byte, SequenceLen),
		},
		State: 0,
	}

	err = c.SetReadDeadline(time.Now().Add(d))
	if err != nil {
		logger.Error(err)
		return
	}

	_, err = io.ReadFull(c, res.R.Sequence)
	if err != nil {
		logger.Error(err)
		return
	}

	s := make([]byte, 1+4)
	err = c.SetReadDeadline(time.Now().Add(1 * time.Second))
	if err != nil {
		logger.Error(err)
		return
	}
	_, err = io.ReadFull(c, s)
	if err != nil {
		logger.Error(err)
		return
	}
	res.State = state(s[0])

	return
}
