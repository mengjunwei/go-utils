package services

import (
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/astaxie/beego"
)

var Host string

func GetLocalIp() (string, error) {
	addr := strings.TrimSpace(beego.AppConfig.String("HostAddr"))
	if len(addr) > 0 {
		Host = addr
		return addr, nil
	}

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		errStr := fmt.Sprintf("get Interface Addr errror:%s", err.Error())
		return "", errors.New(errStr)
	}

	ips := make([]string, 0)
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ipStr := ipnet.IP.String()
				ips = append(ips, ipStr)
			}
		}
	}

	if len(ips) < 1 {
		return "", errors.New("can't get any local addr")
	}
	Host = ips[0]
	return Host, nil
}
