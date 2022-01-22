package protocol

import (
  "errors"
  "fmt"
  "strings"
)

type Token struct {
  ConnId     string // connection id
  HostId string
}

const TokenLen = 32

func (t *Token) String() string {
  id := "_" + t.ConnId
  return t.HostId[:TokenLen-len(id)] + id
}

func ResumeToken(oriData []byte, hostId string) (tk Token, err error) {
  str := string(oriData)
  sp := strings.Split(str, "_")

  if len(sp) != 2 || !strings.HasPrefix(hostId, sp[0]) {
    err = errors.New(fmt.Sprintf("host prefix of Token(%s) error. full hostid is %s, but remote prefix is %s",
      str, hostId, sp[0]))
    return
  }

  return Token{sp[1], hostId}, nil
}

