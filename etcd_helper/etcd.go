package etcd_helper

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/coreos/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/clientv3"

	"github.com/mengjunwei/go-utils/log"
)

const (
	rootPath      = "/go-utils/common"
	leaderPath    = "leader"
	discoveryPath = "discovery"

	sessionTimeout = 20
	dialTimeout    = 5
)

type Conn struct {
	ctx context.Context

	client     *clientv3.Client
	serverList map[string]string
	lock       sync.Mutex

	cbDelay int
	cb      func([]string)
	cbChan  chan []string
}

// 指定client端，Endpoints是etcd server的机器列表，DialTimeout是计算节点链接服务的超时时间
func NewConn(endpoints []string, ctx context.Context) *Conn {
	config := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout * time.Second,
	}
	client, err := clientv3.New(config)
	if err != nil {
		panic(err)
	}
	return &Conn{ctx: ctx, client: client, serverList: make(map[string]string), cbChan: make(chan []string, 16)}
}

//注册服务
func (c *Conn) RegService(key string, val string) error {
	//resp, err := c.client.Get(context.Background(), key)
	//if err != nil {
	//	return err
	//}
	//// resp是从指定prefix服务下get回的value，extractAddrs将value内容存到list
	//if c.existKey(resp, key) {
	//	panic(fmt.Sprintf("service exist, path:%s", key))
	//}
	kv := clientv3.NewKV(c.client)
	ctx := context.Background()
	lease := clientv3.NewLease(c.client)

	//设置租约过期时间为20秒
	leaseRes, err := clientv3.NewLease(c.client).Grant(ctx, sessionTimeout)
	if err != nil {
		return err
	}
	_, err = kv.Put(context.Background(), key, val, clientv3.WithLease(leaseRes.ID)) //把服务的key绑定到租约下面
	if err != nil {
		return err
	}
	//续租时间大概自动为租约的三分之一时间，context.TODO官方定义为是你不知道要传什么
	keepaliveRes, err := lease.KeepAlive(context.TODO(), leaseRes.ID)
	if err != nil {
		return err
	}
	go c.lisKeepAlive(keepaliveRes)
	return err
}

func (c *Conn) lisKeepAlive(keepaliveRes <-chan *clientv3.LeaseKeepAliveResponse) {
	for {
		select {
		case ret := <-keepaliveRes:
			if ret != nil {
				fmt.Println(ret.ID, "续租成功", time.Now())
			}
		case <-c.ctx.Done():
			return
		}
	}
}

// 获取prefix目录下所有内容，并返回
func (c *Conn) GetService(prefix string) ([]string, error) {
	resp, err := c.client.Get(context.Background(), prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	// resp是从指定prefix服务下get回的value，extractAddrs将value内容存到list
	addrs := c.extractAddrs(resp)

	if c.cb != nil {
		if c.cbDelay == 0 {
			c.cb(addrs)
		} else {
			select {
			case c.cbChan <- addrs:
			default:
			}
		}
	}

	go c.watcher(prefix)
	return addrs, nil
}

// 将获取到的prefix目录下所有内容存入list并返回
func (c *Conn) extractAddrs(resp *clientv3.GetResponse) []string {
	addrs := make([]string, 0)
	if resp == nil || resp.Kvs == nil {
		return addrs
	}
	for i := range resp.Kvs {
		if v := resp.Kvs[i].Value; v != nil {
			c.SetServiceList(string(resp.Kvs[i].Key), string(resp.Kvs[i].Value))
			addrs = append(addrs, string(v))
		}
	}
	return addrs
}

func (c *Conn) existKey(resp *clientv3.GetResponse, k string) bool {
	if resp == nil || resp.Kvs == nil {
		return false
	}
	for i := range resp.Kvs {
		if string(resp.Kvs[i].Key) == k {
			return true
		}
	}
	return false
}

// watch负责将监听到的put、delete请求存放到指定list
func (c *Conn) watcher(prefix string) {
	rch := c.client.Watch(context.Background(), prefix, clientv3.WithPrefix())
	select {
	case wresp := <-rch:
		for _, ev := range wresp.Events {
			switch ev.Type {
			case mvccpb.PUT, mvccpb.DELETE:
				_, _ = c.GetService(prefix)
			}
		}
	case <-c.ctx.Done():
		return
	}
}

func (c *Conn) SetServiceList(key, val string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.serverList[key] = string(val)
	log.DebugF("set data key :", key, "val:", val)
}

func (c *Conn) DelServiceList(key string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.serverList, key)
	log.DebugF("del data key:", key)
}

func (c *Conn) SetCallBack(cb func([]string), delay int) {
	if c.cb == nil && cb != nil {
		c.cb = cb
		c.cbDelay = delay
		if delay > 0 {
			go c.callbackDelayLoop()
		}
	}
}

func (c *Conn) callbackDelayLoop() {
	eventsMap := make(map[int64][]string)
	timer := time.NewTimer(time.Duration(c.cbDelay) * time.Second)
	for {
		select {
		case <-c.ctx.Done():
			goto exit
		case event := <-c.cbChan:
			now := time.Now().Unix()
			eventsMap[now] = event
			timer.Reset(time.Duration(c.cbDelay) * time.Second)
			log.DebugF("discovery callbackDelayLoop cb will  be call: %v, now:%d, delay:%d s", event, now, c.cbDelay)

		case <-timer.C:
			now := time.Now().Unix()
			max, event := findMax(eventsMap, now)
			if now-max >= int64(c.cbDelay) {
				if c.cb != nil {
					log.DebugF("cb call now, before: %d", now)
					c.cb(event)
					log.DebugF("cb call now, after: %d", now)
				}
				eventsMap = make(map[int64][]string)
			} else {
				if len(event) != 0 {
					log.WarnF("discovery find event, but can't exec")
				}
			}
			timer.Reset(time.Duration(c.cbDelay) * time.Second)
		}
	}
exit:
	timer.Stop()
	log.Debug("watch callback loop quit")
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
