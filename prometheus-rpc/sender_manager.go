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
	MetricTotal uint64
	MetricOk    uint64
}

type SenderManager struct {
	sync.Mutex
	stopped    bool
	parallel   int
	rpcSenders []*Sender
	index      uint64
	status     Status
	addr       string
	name       string
	HashAddr   []string
	Datasource string
}

func NewSendManager(name, addr string) *SenderManager {
	parallel := defaultParallel

	m := &SenderManager{
		addr:       addr,
		name:       name,
		stopped:    true,
		parallel:   parallel,
		rpcSenders: make([]*Sender, parallel),
	}
	for i, _ := range m.rpcSenders {
		m.rpcSenders[i] = NewSender(m, i, defaultFlushInterval, addr)
	}
	return m
}

func (m *SenderManager) Run() error {
	m.Lock()
	defer m.Unlock()
	if !m.stopped {
		return nil
	}

	for _, s := range m.rpcSenders {
		s.Start()
	}
	m.stopped = false

	log.InfoF("SendManager :%s %s run", m.name, m.addr)
	return nil
}

func (m *SenderManager) Stop() error {
	m.Lock()
	defer m.Unlock()
	if m.stopped {
		return nil
	}

	for _, s := range m.rpcSenders {
		s.Stop()
	}
	m.stopped = true

	log.InfoF("SendManager :%s %s stop", m.name, m.addr)
	return nil
}

func (m *SenderManager) ParseAndSend(typ int, body io.Reader, groupLabels map[string]string) error {
	ms, err := parser.ParseMetrics(typ, body, groupLabels)
	if err != nil {
		return err
	}

	if ms != nil {
		err = m.Send(ms)
	}
	return err
}

func (m *SenderManager) Parse(typ int, body io.Reader, groupLabels map[string]string) (*metrics.Metrics, error) {
	ms, err := parser.ParseMetrics(typ, body, groupLabels)
	if err != nil {
		return nil, err
	}
	return ms, nil
}

func (m *SenderManager) Send(ms *metrics.Metrics) error {
	var err error
	c := len(ms.List)
	if c == 0 {
		return nil
	}

	m.atomicMetricStatus(uint64(c), 0)
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

func (m *SenderManager) send(ms *metrics.Metrics) error {
	if len(ms.List) > 0 {
		i := atomic.AddUint64(&m.index, 1) % uint64(m.parallel)
		return m.rpcSenders[i].Send(ms)
	}
	return nil
}

func (m *SenderManager) atomicMetricStatus(received, send uint64) {
	if received > 0 {
		atomic.AddUint64(&m.status.MetricTotal, received)
	}
	if send > 0 {
		atomic.AddUint64(&m.status.MetricOk, send)
	}
}

func (m *SenderManager) Status() (uint64, uint64) {
	return atomic.LoadUint64(&m.status.MetricTotal), atomic.LoadUint64(&m.status.MetricOk)
}
