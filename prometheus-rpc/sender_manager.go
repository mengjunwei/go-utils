package rpc

import (
	"io"
	"sync"
	"sync/atomic"

	"github.com/mengjunwei/go-utils/log"
	"github.com/mengjunwei/go-utils/prometheus-rpc/gen-go/metrics"
	"github.com/mengjunwei/go-utils/prometheus-rpc/parser"
)

const (
	defaultFlushInterval = 5
	defaultParallel      = 6
	batchNumbers         = 256
)

type Status struct {
	MetricAll    uint64
	MetricSendOk uint64
}

type SendManager struct {
	sync.Mutex
	addr     string
	name     string
	stopped  bool
	parallel int
	senders  []*Sender
	index    uint64
	status   Status

	HashAddr   []string
	Datasource string
}

func NewSendManager(name, addr string) *SendManager {
	parallel := defaultParallel

	m := &SendManager{
		addr:     addr,
		name:     name,
		stopped:  true,
		parallel: parallel,
		senders:  make([]*Sender, parallel),
	}
	for i, _ := range m.senders {
		m.senders[i] = NewSender(m, i, defaultFlushInterval, addr)
	}

	return m
}

func (m *SendManager) Run() error {
	m.Lock()
	defer m.Unlock()
	if !m.stopped {
		return nil
	}

	for _, s := range m.senders {
		s.Start()
	}
	m.stopped = false

	log.InfoF("SendManager :%s %s run", m.name, m.addr)
	return nil
}

func (m *SendManager) Stop() error {
	m.Lock()
	defer m.Unlock()
	if m.stopped {
		return nil
	}

	for _, s := range m.senders {
		s.Stop()
	}
	m.stopped = true

	log.InfoF("SendManager :%s %s stop", m.name, m.addr)
	return nil
}

func (m *SendManager) ParseAndSend(typ int, body io.Reader, groupLabels map[string]string) error {
	ms, err := parser.ParseMetrics(typ, body, groupLabels)
	if err != nil {
		return err
	}

	if ms != nil {
		err = m.Send(ms)
	}
	return err
}

func (m *SendManager) Parse(typ int, body io.Reader, groupLabels map[string]string) (*metrics.Metrics, error) {
	ms, err := parser.ParseMetrics(typ, body, groupLabels)
	if err != nil {
		return nil, err
	}
	return ms, nil
}

func (m *SendManager) Send(ms *metrics.Metrics) error {
	var err error
	c := len(ms.List)
	if c == 0 {
		return nil
	}

	m.metricStatus(uint64(c), 0)
	if c <= batchNumbers {
		return m.send(ms)
	}

	//分批发送
	i := 0
	for {
		end := i + batchNumbers
		if end <= c {
			newMs := &metrics.Metrics{List: ms.List[i:end]}
			tmpErr := m.send(newMs)
			if tmpErr != nil {
				err = tmpErr
			}
		} else {
			if i < c {
				newMs := &metrics.Metrics{List: ms.List[i:c]}
				tmpErr := m.send(newMs)
				if tmpErr != nil {
					err = tmpErr
				}
			}
			break
		}
		i = end
	}
	return err
}

func (m *SendManager) send(ms *metrics.Metrics) error {
	if len(ms.List) > 0 {
		i := atomic.AddUint64(&m.index, 1) % uint64(m.parallel)
		return m.senders[i].Send(ms)
	}

	return nil
}

func (m *SendManager) metricStatus(received, send uint64) {
	if received > 0 {
		atomic.AddUint64(&m.status.MetricAll, received)
	}

	if send > 0 {
		atomic.AddUint64(&m.status.MetricSendOk, send)
	}
}

func (m *SendManager) Status() (uint64, uint64) {
	return atomic.LoadUint64(&m.status.MetricAll), atomic.LoadUint64(&m.status.MetricSendOk)
}
