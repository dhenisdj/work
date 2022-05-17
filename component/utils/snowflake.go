package utils

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	rand2 "math/rand"
	"net"
	"os"
	"sync/atomic"
	"time"
)

var _DEFAULT SnowFlake

func GetServerId(host string, port uint16) uint32 {
	bigInt := big.NewInt(0)
	ipv4 := net.ParseIP(host).To4()
	for i, j := 0, len(ipv4)-1; i < j; i, j = i+1, j-1 {
		ipv4[i], ipv4[j] = ipv4[j], ipv4[i]
	}
	bigInt.SetBytes(ipv4)

	return uint32(bigInt.Int64()) + uint32(port)
}

type SnowFlake interface {
	Sequence() string
}

func Sequence() string {
	if _DEFAULT == nil {
		host, err := GetIP()
		if err != nil {
			host = "localhost"
		}
		pid := os.Getpid()
		_DEFAULT = NewSnowFlake(host, uint16(pid))
	}
	return _DEFAULT.Sequence()
}

func NewSnowFlake(host string, port uint16) SnowFlake {
	r := rand2.New(rand2.NewSource(time.Now().UnixNano()))
	return &snowFlake{
		serverId: GetServerId(host, port),
		sequence: r.Uint32(),
		rand:     r,
	}
}

type snowFlake struct {
	serverId uint32
	sequence uint32
	rand     *rand2.Rand
}

func (that *snowFlake) Sequence() string {
	now := time.Now()
	seq := atomic.AddUint32(&that.sequence, 1)

	buffer := [12]byte{}
	binary.BigEndian.PutUint32(buffer[0:4], uint32(that.serverId))
	binary.BigEndian.PutUint32(buffer[4:8], uint32(now.UnixNano()/int64(time.Millisecond)))
	binary.BigEndian.PutUint16(buffer[8:10], uint16(seq))
	binary.BigEndian.PutUint16(buffer[10:12], uint16(that.rand.Intn(math.MaxUint16)))

	return fmt.Sprintf("%x", buffer)
}
