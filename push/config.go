package push

import (
	"github.com/xpwu/go-config/configs"
	"github.com/xpwu/go-xnet/xtcp"
)

type config struct {
	Servers []*server
}

type server struct {
	Net                *xtcp.Net
	CloseSubProtocolId byte
	DataSubProtocolId  byte
}

var configValue = &config{
	Servers: []*server{
		{
			Net:                xtcp.DefaultNetConfig(),
			CloseSubProtocolId: 1,
			DataSubProtocolId:  0,
		},
	},
}

func init() {
	configs.Unmarshal(configValue)
}
