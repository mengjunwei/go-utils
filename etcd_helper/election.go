package etcd_helper

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/mengjunwei/go-utils/log"
)

type Election struct {
	sync.Mutex
	start       bool
	ctx         context.Context
	master      bool
	app         string
	addr        string
	metaData    string
	path        string
	etcdServers []string
	etcdConn    *Conn
	cb          func(interface{})
}

func NewElection(etcdServers []string, appName, addr, metaData string, ctx context.Context) (*Election, error) {
	if len(etcdServers) == 0 {
		return nil, errors.New("NewElection etcdServers is nil")
	}

	if len(appName) == 0 {
		return nil, errors.New("NewElection etcdServers is nil")
	}

	path := fmt.Sprintf("%s/%s/%s", rootPath, appName, leaderPath)

	e := &Election{
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

func (e *Election) Start() error {
	e.Lock()
	if e.start {
		e.Unlock()
		return nil
	}
	e.Unlock()

	// 初始化
	childPath := fmt.Sprintf("%s/%s", e.path, e.addr)
	if err := e.etcdConn.RegService(childPath, e.metaData); err != nil {
		e.etcdConn.client.Close()
		return fmt.Errorf("election RegService path: %w", err)
	}

	e.Lock()
	e.start = true
	e.Unlock()

	e.etcdConn.SetCallBack(e.election, 3)

	// 开始
	if _, err := e.etcdConn.GetService(e.path); err != nil {
		log.Error(err.Error())
		return err
	}

	return nil
}

func (e *Election) Stop() {
	e.Lock()
	defer e.Unlock()
	if !e.start {
		return
	}
	e.start = false

	//删除临时节点
	if e.etcdConn != nil {
		childPath := fmt.Sprintf("%s/%s", e.path, e.addr)
		_, err := e.etcdConn.client.Delete(context.Background(), childPath)
		if err != nil {
			log.Error(err.Error())
		}
	}

	if e.etcdConn != nil {
		e.etcdConn.client.Close()
		e.etcdConn = nil
	}
}

func (e *Election) election(instances []string) {
	if len(instances) < 1 {
		return
	}
	sort.Strings(instances)
	if e.metaData == instances[0] {
		e.Lock()
		e.master = true
		if e.cb != nil {
			e.cb(nil)
		}
		e.Unlock()
	} else {
		e.Lock()
		e.master = false
		if e.cb != nil {
			e.cb(nil)
		}
		e.Unlock()
	}
}

func (e *Election) Master() bool {
	e.Lock()
	defer e.Unlock()
	return e.master
}

func (e *Election) IsStop() bool {
	e.Lock()
	defer e.Unlock()
	return e.start == false
}

func (e *Election) MasterCallBack(cb func(interface{})) {
	e.cb = cb
}
