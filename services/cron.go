package services

import (
	"errors"
	"fmt"
	"hash/crc32"
	"net"
	"sync"
)

type CronService struct {
	sync.Mutex
	Stopped      bool
	SyncInterval int
	Seq          int
	Hosts        []string
	EnvLevel     string
}

func (service *CronService) GetConfigIp() ([]string, error) {
	if service.EnvLevel == "test" {
		localIp, err := service.GetLocalIp()
		if err != nil {
			return nil, err
		}
		service.Hosts = localIp
	}

	return service.Hosts, nil
}

func (service *CronService) GetLocalIp() ([]string, error) {
	addrSlice, err := net.InterfaceAddrs()
	if err != nil {
		errStr := fmt.Sprintf("get Interface Addr errror:%s", err.Error())
		return nil, errors.New(errStr)
	}

	ips := make([]string, 0)
	for _, address := range addrSlice {
		if ipNet, ok := address.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				ipStr := ipNet.IP.String()
				ips = append(ips, ipStr)
			}
		}
	}

	if len(ips) < 1 {
		return nil, errors.New("Addr length less than 1")
	}
	return ips, nil
}

func (service *CronService) IsNecessaryExecute(serviceName string) (bool, error) {
	serviceHash := Crc32([]byte(serviceName))
	_, err := service.GetConfigIp()
	if err != nil {
		return false, err
	}

	service.Seq = int(serviceHash) % len(service.Hosts)
	localIps, err := service.GetLocalIp()
	logInstance.Info("local IP: %v", localIps)
	if err != nil {
		return false, err
	}

	for _, ip := range localIps {
		if ip == service.Hosts[service.Seq] {
			logInstance.Info("sync task name: %s; ip: %s", serviceName, localIps[0])
			return true, nil
		}
	}

	return false, nil
}

func Crc32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
