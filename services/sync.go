package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ylywyn/restclient"

	"github.com/mengjunwei/go-utils/log"
)

const ServiceSync = "SyncService"

//数据同步服务
type SyncService struct {
	sync.Mutex
	stopped   bool
	graceShut chan struct{}
	ctx       context.Context

	tasks map[string]*SyncTask
}

type HttpResp struct {
	Result bool          `json:"result"`
	Err    string        `json:"error"`
	Data   *HttpRespData `json:"data"`
}

type HttpRespData struct {
	Data      []json.RawMessage
	Timestamp int64
	Count     int
}

type SyncTask struct {
	Name     string
	Interval int
	Version  int64
	Page     int
	PageSize int
	Url      string

	UrlCallBackFun  func(t *SyncTask) string
	SyncCallBackFun func(t *SyncTask, data []json.RawMessage) error
}

func NewSyncService() *SyncService {
	m := &SyncService{
		stopped:   true,
		graceShut: make(chan struct{}),
		tasks:     make(map[string]*SyncTask),
	}
	return m
}

func (s *SyncService) Init() error {
	return nil
}

func (s *SyncService) AddTask(t *SyncTask) {
	if t == nil {
		return
	}
	s.Lock()
	defer s.Unlock()

	if len(t.Name) > 0 {
		s.tasks[t.Name] = t
	}

	//已经启动了
	if !s.stopped {
		if err := s.syncLoop(false, t); err != nil {
			log.ErrorF("sync service task: %s error: %s", t.Name, err.Error())
		}
		go s.syncLoop(true, t)
	}
}

func (s *SyncService) Run(ctx context.Context) error {
	s.Lock()
	if !s.stopped {
		s.Unlock()
		return nil
	}
	s.stopped = false
	s.ctx = ctx
	s.Unlock()

	for _, task := range s.tasks {
		//先同步一把数据，使在程序完全启动前， 保证是有数据的
		if err := s.syncLoop(false, task); err != nil {
			log.ErrorF("sync service task: %s error: %s", task.Name, err.Error())
		}
		go s.syncLoop(true, task)
	}

	return nil
}

func (s *SyncService) Stop() {
	s.Lock()
	defer s.Unlock()
	if s.stopped {
		return
	}

	s.stopped = true
	close(s.graceShut)
}

func (s *SyncService) syncLoop(loop bool, t *SyncTask) error {
	interval := t.Interval
	if interval < 1 {
		interval = 1
	}
	ticker := time.NewTicker(time.Duration(interval) * time.Minute)
	defer ticker.Stop()

	syncFun := func() error {
		resp, err := s.syncAll(t)
		if err != nil {
			log.Error(err.Error())
			return err
		}

		if resp == nil || len(resp.Data) == 0 {
			log.DebugF("sync service task: %s sync item 0", t.Name)
			return nil
		}

		if t.SyncCallBackFun != nil {
			t.SyncCallBackFun(t, resp.Data)
		}
		t.Version = resp.Timestamp
		log.InfoF("sync service task: %s  count: %d, version: %d", t.Name, len(resp.Data), t.Version)
		return nil
	}

	if !loop {
		return syncFun()
	}

	log.InfoF("sync service task: %s  loop run, interval:%d", t.Name, interval)
	for {
		select {
		case <-ticker.C:
		case <-s.graceShut:
			log.InfoF("sync service task: %s  loop quit", t.Name)
			return nil
		case <-s.ctx.Done():
			log.InfoF("sync service task: %s  loop quit", t.Name)
			return nil
		}

		syncFun()
	}
}

func (s *SyncService) syncAll(t *SyncTask) (*HttpRespData, error) {
	if t.PageSize == 0 {
		return s.sync(t)
	}

	t.Page = 1
	rets, err := s.sync(t)
	if err != nil {
		return nil, err
	}
	if rets.Count == 0 {
		return nil, nil
	}

	firstTimestamp := rets.Timestamp
	all := rets.Count
	cur := len(rets.Data)
	for {
		if cur >= all {
			break
		}

		t.Page += 1
		temp, err := s.sync(t)
		if err != nil {
			return nil, err
		}
		if temp.Timestamp != firstTimestamp {
			return nil, errors.New("get all firstTimestamp is modify")
		}
		num := len(temp.Data)
		if num == 0 {
			break
		}

		rets.Data = append(rets.Data, temp.Data...)
		if num < t.PageSize {
			break
		}
		cur += len(temp.Data)
	}

	return rets, nil
}

func (s *SyncService) sync(t *SyncTask) (*HttpRespData, error) {
	url := t.Url
	if t.UrlCallBackFun != nil {
		url = t.UrlCallBackFun(t)
	}
	respData := &HttpRespData{}
	resp := &HttpResp{Data: respData}
	httpResp := restclient.Get(url).NoLogger().SendAndGetJsonResponse(resp)
	if err := httpResp.Error(); err != nil {
		return nil, fmt.Errorf("sync service task %s: %w", t.Name, err)
	}

	if len(resp.Err) > 0 {
		return nil, fmt.Errorf("sync service task %s: %s", t.Name, resp.Err)
	}

	return resp.Data, nil
}
