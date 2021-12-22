package zkhelper

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type Discovery struct {
	sync.Mutex
	start      bool
	ctx        context.Context
	watchCount int

	app       string
	addr      string
	path      string
	zkServers []string
	zkConn    *zk.Conn

	cbDelay int
	cb      func([]string)
	cbChan  chan []string
}

func NewDiscovery(zkServers []string, appName, addr string, ctx context.Context) (*Discovery, error) {
	if len(zkServers) == 0 {
		return nil, errors.New("NewDiscovery zkServers is nil")
	}

	if len(appName) == 0 {
		return nil, errors.New("NewDiscovery appName is nil")
	}

	path := fmt.Sprintf("%s/%s/%s", rootPath, appName, discoveryPath)

	e := &Discovery{
		ctx:       ctx,
		start:     false,
		app:       appName,
		addr:      addr,
		path:      path,
		zkServers: zkServers,
		cbChan:    make(chan []string, 16),
	}
	return e, nil
}

func (e *Discovery) Start() error {
	e.Lock()
	if e.start {
		e.Unlock()
		return nil
	}
	e.Unlock()

	// 初始化
	conn, _, err := zk.Connect(e.zkServers, sessionTimeout*time.Second)
	if err != nil {
		return fmt.Errorf("discovery start connect: %w", err)
	}

	if _, err := makePath(e.path, conn, 0, zk.WorldACL(zk.PermAll)); err != nil {
		conn.Close()
		return fmt.Errorf("discovery makePath: %w", err)
	}
	e.zkConn = conn

	e.Lock()
	e.start = true
	e.Unlock()

	// 开始
	if err := e.discovery(); err != nil {
		return err
	}

	return nil
}

func (e *Discovery) Stop() {
	e.Lock()
	defer e.Unlock()
	if !e.start {
		return
	}
	e.start = false

	//删除临时节点
	if e.zkConn != nil {
		childPath := fmt.Sprintf("%s/%s", e.path, e.addr)
		exist, stat, err := e.zkConn.Exists(childPath)
		if exist && err == nil {
			e.zkConn.Delete(e.path, stat.Version)
		}
	}

	if e.zkConn != nil {
		e.zkConn.Close()
		e.zkConn = nil
	}
}

func (e *Discovery) SetCallBack(cb func([]string), delay int) {
	if e.cb == nil && cb != nil {
		e.cb = cb
		e.cbDelay = delay
		if delay > 0 {
			go e.callbackDelayLoop()
		}
	}
}

func (e *Discovery) discovery() error {
	e.Lock()
	if !e.start {
		e.Unlock()
		return nil
	}
	e.Unlock()

	childPath := fmt.Sprintf("%s/%s", e.path, e.addr)
	exist, stat, err := e.zkConn.Exists(childPath)
	if exist && err == nil {
		e.zkConn.Delete(childPath, stat.Version)
	}

	_, err = e.zkConn.Create(childPath, []byte(e.addr), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil && err != zk.ErrNodeExists {
		err = fmt.Errorf("discovery create FlagEphemeral: %w", err)
		logInstance.Error(err.Error())
		time.Sleep(time.Second * 5)
		panic(err.Error())
	}

	e.startWatch()

	logInstance.Info("%s is add to watching.", e.addr)
	return nil
}

func (e *Discovery) startWatch() {
	e.watchCount += 1
	logInstance.Info("discovery watch count: %d", e.watchCount)

	children, _, event, err := e.zkConn.ChildrenW(e.path)
	if err != nil {
		err = fmt.Errorf("discovery ChildrenW: %w", err)
		logInstance.Error(err.Error())
		time.Sleep(time.Second * 5)
		panic(err.Error())
	}
	if e.cb != nil {
		if e.cbDelay == 0 {
			e.cb(children)
		} else {
			select {
			case e.cbChan <- children:
			default:
			}
		}
	}

	go e.watchEvent(event)
}

func (e *Discovery) watchEvent(ech <-chan zk.Event) {
	e.Lock()
	if !e.start {
		e.Unlock()
		return
	}
	e.Unlock()

	count := e.watchCount
	logInstance.Debug("discovery watchEvent begin, watch count: %d", count)

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

		if event.Type == zk.EventNodeChildrenChanged {
			logInstance.Debug("discovery watch action...")
			e.startWatch()
		}

	case <-e.ctx.Done():
		e.Stop()
	}

	logInstance.Debug("discovery watchEvent quit, watch count: %d", count)
}

func (e *Discovery) callbackDelayLoop() {
	eventsMap := make(map[int64][]string)
	timer := time.NewTimer(time.Duration(e.cbDelay) * time.Second)
	for {
		select {
		case <-e.ctx.Done():
			e.Stop()
			goto exit
		case event := <-e.cbChan:
			now := time.Now().Unix()
			eventsMap[now] = event
			timer.Reset(time.Duration(e.cbDelay) * time.Second)
			logInstance.Debug("discovery callbackDelayLoop cb will  be call: %v, now:%d, delay:%d s", event, now, e.cbDelay)

		case <-timer.C:
			now := time.Now().Unix()
			max, event := findMax(eventsMap, now)
			if now-max >= int64(e.cbDelay) {
				if e.cb != nil {
					logInstance.Debug("cb call now, before: %d", now)
					e.cb(event)
					logInstance.Debug("cb call now, after: %d", now)
				}
				eventsMap = make(map[int64][]string)
			} else {
				if len(event) != 0 {
					logInstance.Warning("discovery find event, but can't exec")
				}
			}
			timer.Reset(time.Duration(e.cbDelay) * time.Second)
		}
	}
exit:
	timer.Stop()
	logInstance.Debug("watch callback loop quit")
}

func findMax(maps map[int64][]string, now int64) (int64, []string) {
	if len(maps) == 0 {
		return now, nil
	}

	var key int64 = 0
	var events []string
	for k, v := range maps {
		if k >= key {
			key = k
			events = v
		}
	}
	return key, events
}
