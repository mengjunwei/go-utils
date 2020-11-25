package zkhelper

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

func ExampleElection() {
	var zkServers = []string{"10.168.99.104:2181"}

	t := time.Now().Unix()
	host := strconv.FormatInt(t, 10)

	ctx := context.Background()
	e, _ := NewElection(zkServers, "testleader", host, ctx)
	e.MasterCallBack(callBack)
	if err := e.Start(); err != nil {
		return
	}
}

func callBack(master string) {
	fmt.Printf("callback: master is %s\n", master)
}

func ExampleDiscovery() {
	var zkServers = []string{"10.168.99.104:2181"}

	t := time.Now().Unix()
	host := strconv.FormatInt(t, 10)

	ctx := context.Background()
	e, _ := NewDiscovery(zkServers, "testleader", host, ctx)
	
	//delay设置一个延时，用于合并callback(应对滚动升级那一点时间时, 服务发现会不稳定)
	e.SetCallBack(callBackDiscovery, 15)
	if err := e.Start(); err != nil {
		return
	}
}

func callBackDiscovery(events []string) {
	fmt.Printf("ExampleDiscovery callback:  is %v\n", events)
}
