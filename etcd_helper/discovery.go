package etcd_helper

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type Discovery struct {
	sync.Mutex
	start      bool
	ctx        context.Context
	watchCount int

	app         string
	addr        string
	metaData    string
	path        string
	etcdServers []string
	etcdConn    *Conn
}

func NewDiscovery(etcdServers []string, appName, addr, metaData string, ctx context.Context) (*Discovery, error) {
	if len(etcdServers) == 0 {
		return nil, errors.New("NewDiscovery etcdServers is nil")
	}

	if len(appName) == 0 {
		return nil, errors.New("NewDiscovery etcdServers is nil")
	}

	path := fmt.Sprintf("%s/%s/%s", rootPath, appName, discoveryPath)

	e := &Discovery{
		ctx:         ctx,
		start:       false,
		app:         appName,
		addr:        addr,
		metaData:    metaData,
		path:        path,
		etcdServers: etcdServers,
	}
	conn := NewConn(e.etcdServers, e.ctx)
	e.etcdConn = conn
	return e, nil
}

func (d *Discovery) Start() error {
	d.Lock()
	if d.start {
		d.Unlock()
		return nil
	}
	d.Unlock()

	// 初始化
	childPath := fmt.Sprintf("%s/%s", d.path, d.addr)
	if err := d.etcdConn.RegService(childPath, d.metaData); err != nil {
		d.etcdConn.client.Close()
		return fmt.Errorf("discovery RegService path: %w", err)
	}

	d.Lock()
	d.start = true
	d.Unlock()

	// 开始
	if _, err := d.etcdConn.GetService(d.path); err != nil {
		return err
	}

	return nil
}

func (d *Discovery) Stop() {
	d.Lock()
	defer d.Unlock()
	if !d.start {
		return
	}
	d.start = false

	//删除临时节点
	if d.etcdConn != nil {
		childPath := fmt.Sprintf("%s/%s", d.path, d.addr)
		_, err := d.etcdConn.client.Delete(context.Background(), childPath)
		if err != nil {
			fmt.Println(err)
		}
	}

	if d.etcdConn != nil {
		d.etcdConn.client.Close()
		d.etcdConn = nil
	}
}

func (d *Discovery) SetCallBack(cb func([]string), delay int) {
	d.etcdConn.SetCallBack(cb, delay)
}
