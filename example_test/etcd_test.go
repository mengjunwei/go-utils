package example

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/mengjunwei/go-utils/etcd_helper"
)

func TestEtcdHelperElection(t *testing.T) {
	var etcdServers = []string{"192.168.87.113:2379"}

	tNow := time.Now().Unix()
	host := strconv.FormatInt(tNow, 10)

	ctx := context.Background()
	e, _ := etcd_helper.NewElection(etcdServers, "testleader", host, host, ctx)
	if err := e.Start(); err != nil {

	}
	e.MasterCallBack(callBackElection)
	if err := e.Start(); err != nil {
		return
	}
	select {}
}

func callBackElection(data interface{}) {
	fmt.Printf("callback: master is %+v", data)
}

func TestEtcdHelperDisvovery(t *testing.T) {
	var zkServers = []string{"192.168.87.113:2379"}

	tNow := time.Now().Unix()
	host := strconv.FormatInt(tNow, 10)

	ctx := context.Background()
	e, _ := etcd_helper.NewDiscovery(zkServers, "testleader", host, host, ctx)
	e.SetCallBack(callBackDiscovery, 3)
	if err := e.Start(); err != nil {
		return
	}
	select {}
}

func TestEtcdHelperDisvoveryV1(t *testing.T) {
	var zkServers = []string{"192.168.87.113:2379"}

	tNow := time.Now().Unix()
	host := strconv.FormatInt(tNow, 10)

	ctx := context.Background()
	e, _ := etcd_helper.NewDiscovery(zkServers, "testleader", host, host, ctx)
	e.SetCallBack(callBackDiscovery, 3)
	if err := e.Start(); err != nil {
		return
	}
	select {}
}

func callBackDiscovery(data []string) {
	fmt.Printf("diskcovery is %+v", data)
}

func TestEtcdHelperElectionV1(t *testing.T) {
	var etcdServers = []string{"192.168.87.113:2379"}

	tNow := time.Now().Unix()
	host := strconv.FormatInt(tNow, 10)

	ctx := context.Background()
	e, _ := etcd_helper.NewElection(etcdServers, "testleader", host, host, ctx)
	if err := e.Start(); err != nil {

	}
	e.MasterCallBack(callBackElection)
	if err := e.Start(); err != nil {
		return
	}
	select {}
}

func TestEtcdHelperElectionV2(t *testing.T) {
	var etcdServers = []string{"192.168.87.113:2379"}

	host := "22.22.22.22"

	ctx := context.Background()
	e, _ := etcd_helper.NewElection(etcdServers, "testleader", host, host, ctx)
	if err := e.Start(); err != nil {

	}
	e.MasterCallBack(callBackElection)
	if err := e.Start(); err != nil {
		return
	}
	select {}
}

func TestEtcdHelperElectionV3(t *testing.T) {
	var etcdServers = []string{"192.168.87.113:2379"}

	host := "22.22.22.22"

	ctx := context.Background()
	e, _ := etcd_helper.NewElection(etcdServers, "testleader", host, host, ctx)
	if err := e.Start(); err != nil {

	}
	e.MasterCallBack(callBackElection)
	if err := e.Start(); err != nil {
		return
	}
	select {}
}
