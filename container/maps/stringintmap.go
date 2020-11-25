package maps

import (
	"sync"
)

type StringIntMap struct {
	sync.RWMutex
	M map[string]int
}

func NewStringIntMap() *StringIntMap {
	return &StringIntMap{M: make(map[string]int)}
}

func (s *StringIntMap) Put(key string, val int) {
	s.Lock()
	defer s.Unlock()
	s.M[key] = val
}

func (s *StringIntMap) Puts(m map[string]int) {
	todo := make(map[string]int)
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

func (s *StringIntMap) Get(key string) (int, bool) {
	s.RLock()
	defer s.RUnlock()
	val, exists := s.M[key]
	return val, exists
}

func (s *StringIntMap) Exists(key string) bool {
	_, exists := s.Get(key)
	return exists
}

func (s *StringIntMap) Remove(key string) {
	s.Lock()
	defer s.Unlock()
	delete(s.M, key)
}
