package redis

import (
	"errors"
	"time"

	"github.com/garyburd/redigo/redis"

	"github.com/mengjunwei/go-utils/log"
)

var Pool *ClientPool

//connInfo : 127.0.0.1:6379
func InitPool(connInfo string) error {
	log.DebugF("redis 地址 :%s", connInfo)

	Pool = &ClientPool{
		Name: "redis-client-pool",
	}
	err := Pool.InitRedis(connInfo)
	if err != nil {
		log.ErrorF("redis 初始化失败:", err.Error())
	} else {
		log.Info("redis 连接OK")
	}
	return err
}

type ClientPool struct {
	addr string
	Name string
	pool *redis.Pool
}

func (p *ClientPool) InitRedis(addr string) error {
	dialFunc := func() (c redis.Conn, err error) {
		c, err = redis.Dial("tcp", addr)
		if err != nil {
			return
		}
		_, err = c.Do("SELECT", 0)
		if err != nil {
			_ = c.Close()
			return nil, err
		}
		return c, nil
	}
	p.addr = addr

	// initialize a new pool
	p.pool = &redis.Pool{
		MaxIdle:     6,
		IdleTimeout: 240 * time.Second,
		Dial:        dialFunc,
	}

	//test
	if p.Get() != nil {
		return nil
	}
	return errors.New("redis 连接失败")
}

func (p *ClientPool) Get() redis.Conn {
	if p.pool != nil {
		return p.pool.Get()
	}
	return nil
}

func (p *ClientPool) Close() {
	_ = p.pool.Close()
}
