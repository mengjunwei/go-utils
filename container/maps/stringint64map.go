package maps

import (
	"sync"
)

type StringInt64Map struct {
	sync.RWMutex
	M map[string]int64
}

func NewStringInt64Map() *StringInt64Map {
	return &StringInt64Map{M: make(map[string]int64)}
}

func (s *StringInt64Map) Put(key string, val int64) {
	s.Lock()
	defer s.Unlock()
	s.M[key] = val
}

func (s *StringInt64Map) Puts(m map[string]int64) {
	todo := make(map[string]int64)
	s.RLock()
	for k, v := range m {
		old, exists := s.M[k]
		if exists && v == old {
			continue
		}
		todo[k] = v
	}
	s.RUnlock()

	if len(todo) == 0 {
		return
	}

	s.Lock()
	for k, v := range todo {
		s.M[k] = v
	}
	s.Unlock()
}

func (s *StringInt64Map) Get(key string) (int64, bool) {
	s.RLock()
	defer s.RUnlock()
	val, exists := s.M[key]
	return val, exists
}

func (s *StringInt64Map) Exists(key string) bool {
	_, exists := s.Get(key)
	return exists
}

func (s *StringInt64Map) Remove(key string) {
	s.Lock()
	defer s.Unlock()
	delete(s.M, key)
}
