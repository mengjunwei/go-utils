package rpc

import (
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/mengjunwei/go-utils/bytes-util"
	"github.com/mengjunwei/go-utils/log"
	"github.com/mengjunwei/go-utils/prometheus-rpc/gen-go/metrics"
)

const needSendFlag = "__"

type bufferClient struct {
	client        *Client
	buf           sizeBuffer
	lastFlushTime int64
}

type sizeBuffer struct {
	buf  []*metrics.Metric
	size int
}

func (b *sizeBuffer) add(m *metrics.Metric) {
	b.buf[b.size] = m
	b.size += 1
}

func newBufferClient(manager *SenderManager, i int, addr string) (*bufferClient, error) {
	c, err := NewClient(manager, i, addr)
	if err != nil {
		return nil, err
	}
	buf := sizeBuffer{
		buf:  make([]*metrics.Metric, batchNumbers*2),
		size: 0,
	}

	ret := &bufferClient{
		client: c,
		buf:    buf,
	}
	return ret, nil
}

func (c *bufferClient) tryToSend(m *metrics.Metric) {
	c.buf.add(m)
	if c.buf.size >= batchNumbers {
		c.send()
	}
}
func (c *bufferClient) send() {
	ms := &metrics.Metrics{List: c.buf.buf[0:c.buf.size]}
	if err := c.client.Send(ms); err != nil {
		log.ErrorF("send to judge error:%s", err.Error())
	}
	c.buf.size = 0
	c.lastFlushTime = time.Now().Unix()
}

func (c *bufferClient) flush(now int64) {
	if now-c.lastFlushTime >= 6 && c.buf.size > 0 {
		c.send()
	}
}

type HashClient struct {
	seq      int
	clients  []*bufferClient
	dataChan chan *metrics.Metrics
	adders   []string
	manager  *SenderManager
}

func NewHashClient(manager *SenderManager, seq int, hashAddrs []string) (*HashClient, error) {
	sort.Strings(hashAddrs)
	count := len(hashAddrs)
	clients := make([]*bufferClient, 0, count)
	for i := 0; i < count; i++ {
		client, err := newBufferClient(manager, i, hashAddrs[i])
		if err != nil {
			return nil, err
		}
		clients = append(clients, client)
	}

	hc := &HashClient{
		seq:      seq,
		clients:  clients,
		adders:   hashAddrs,
		dataChan: make(chan *metrics.Metrics, batchNumbers),
		manager:  manager,
	}

	go hc.sendLoop()
	log.InfoF("ds:%s hash client %d create, hash addr: %v", manager.name, seq, hashAddrs)
	return hc, nil
}

func (hc *HashClient) Send(ms *metrics.Metrics) error {
	select {
	case hc.dataChan <- ms:
	default:
		return errors.New("sender data channel is full")
	}
	return nil
}

func (hc *HashClient) sendLoop() error {
	t := time.NewTicker(time.Duration(5) * time.Second)
	defer t.Stop()

	count := uint32(len(hc.clients))
	for {
		select {
		case ms, ok := <-hc.dataChan:
			if !ok {
				log.Debug("send loop quit")
				return nil
			}

			if ms != nil {
				for _, m := range ms.List {
					if strings.Index(m.MetricKey, needSendFlag) > 0 {
						pos := Murmur3(bytes_util.ToUnsafeBytes(m.MetricKey)) % count
						hc.clients[pos].tryToSend(m)
					}
				}
			}
		case <-t.C:
			hc.flush()
		}
	}
}

func (hc *HashClient) flush() {
	now := time.Now().Unix()
	for i := 0; i < len(hc.adders); i++ {
		hc.clients[i].flush(now)
	}
}

func (hc *HashClient) Close() {
	if len(hc.clients) > 0 {
		for _, c := range hc.clients {
			c.client.Close()
		}
	}
}