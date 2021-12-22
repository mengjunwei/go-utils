package zkhelper

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type Election struct {
	sync.Mutex
	start      bool
	ctx        context.Context
	watchCount int

	master    bool
	app       string
	addr      string
	path      string
	zkServers []string
	zkConn    *zk.Conn

	cb func(string)
}

//appName: 应用名
//addr: 地址。 ip:port
func NewElection(zkServers []string, appName, addr string, ctx context.Context) (*Election, error) {
	if len(zkServers) == 0 {
		return nil, errors.New("NewElection zkServers is nil")
	}

	if len(appName) == 0 {
		return nil, errors.New("NewElection appName is nil")
	}

	path := fmt.Sprintf("%s/%s/%s", rootPath, appName, leaderPath)

	e := &Election{
		ctx:       ctx,
		start:     false,
		app:       appName,
		addr:      addr,
		path:      path,
		zkServers: zkServers,
	}
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
	conn, _, err := zk.Connect(e.zkServers, sessionTimeout*time.Second)
	if err != nil {
		return fmt.Errorf("election start connect: %w", err)
	}

	root := fmt.Sprintf("%s/%s", rootPath, e.app)
	if _, err := makePath(root, conn, 0, zk.WorldACL(zk.PermAll)); err != nil {
		conn.Close()
		return fmt.Errorf("election makePath: %w", err)
	}

	e.zkConn = conn
	e.Lock()
	e.start = true
	e.Unlock()

	// 开始
	if err := e.election(); err != nil {
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

	if e.master {
		//删除临时节点
		if e.zkConn != nil {
			exist, stat, err := e.zkConn.Exists(e.path)
			if exist && err == nil {
				e.zkConn.Delete(e.path, stat.Version)
			}
		}
	}

	if e.zkConn != nil {
		e.zkConn.Close()
		e.zkConn = nil
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

func (e *Election) MasterCallBack(cb func(string)) {
	e.cb = cb
}

func (e *Election) election() error {
	e.Lock()
	if !e.start {
		e.Unlock()
		return nil
	}
	e.Unlock()

	acls := zk.WorldACL(zk.PermAll)
	_, err := e.zkConn.Create(e.path, []byte(e.addr), zk.FlagEphemeral, acls)
	if err == nil {
		e.Lock()
		e.master = true
		e.Unlock()

		e.startWatch()
		if e.cb != nil {
			e.cb(e.addr)
		}
		logInstance.Info("master is: %s", e.addr)
		return nil
	}

	if err != zk.ErrNodeExists {
		err = fmt.Errorf("election create: %w", err)
		logInstance.Error(err.Error())
		time.Sleep(time.Second * 5)
		panic(err.Error())
	}

	v, _, err := e.zkConn.Get(e.path)
	if err != nil {
		err = fmt.Errorf("election get: %w", err)
		logInstance.Error(err.Error())
		time.Sleep(time.Second * 5)
		panic(err.Error())
	}

	master := string(v)
	if master == e.addr {
		//主节点重启太快, 临时数据没来得及清除
		logInstance.Warning("election ephemeral data exist!")
		//time.Sleep(time.Second*sessionTimeout + 1)
		//panic("election ephemeral data exist! please wait")
	}

	e.Lock()
	e.master = false
	e.Unlock()

	e.startWatch()
	logInstance.Info("%s is not master(master is :%s).", e.addr, master)
	return nil
}

func (e *Election) startWatch() {
	e.watchCount += 1
	logInstance.Info("election watch count: %d", e.watchCount)

	exists, _, event, err := e.zkConn.ExistsW(e.path)
	if !exists {
		err = fmt.Errorf("election watch no node")
		logInstance.Error(err.Error())
		time.Sleep(time.Second * 5)
		panic(err.Error())
	}

	if err != nil {
		err = fmt.Errorf("election watch: %w", err)
		logInstance.Error(err.Error())
		time.Sleep(time.Second * 5)
		panic(err.Error())
	}

	go e.watchEvent(event)
}

func (e *Election) watchEvent(ech <-chan zk.Event) {
	e.Lock()
	if !e.start {
		e.Unlock()
		return
	}
	e.Unlock()

	count := e.watchCount
	logInstance.Debug("election watchEvent begin, watch count: %d", count)

	select {
	case event := <-ech:
		e.Lock()
		if !e.start {
			e.Unlock()
			return
		}
		e.Unlock()

		logInstance.Debug("*******************")
		logInstance.Debug("path: %s", event.Path)
		logInstance.Debug("type: %s", event.Type.String())
		logInstance.Debug("state: %s", event.State.String())
		logInstance.Debug("*******************")

		if event.Type == zk.EventNodeDeleted {
			logInstance.Debug("election watch action...")
			if err := e.election(); err != nil {
				logInstance.Debug("watchElection %s", err.Error())
			}
		}

	case <-e.ctx.Done():
		e.Stop()
	}

	logInstance.Debug("election watchEvent quit, watch count: %d", count)
}
