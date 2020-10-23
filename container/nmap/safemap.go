package nmap

import (
	"sync"
)

type SafeMap struct {
	sync.RWMutex
	M map[string]interface{}
}

func NewSafeMap() *SafeMap {
	return &SafeMap{
		M: make(map[string]interface{}),
	}
}

func (s *SafeMap) Put(key string, val interface{}) {
	s.Lock()
	s.M[key] = val
	s.Unlock()
}

func (s *SafeMap) Get(key string) (interface{}, bool) {
	s.RLock()
	val, exists := s.M[key]
	s.RUnlock()
	return val, exists
}

func (s *SafeMap) Remove(key string) {
	s.Lock()
	delete(s.M, key)
	s.Unlock()
}

func (s *SafeMap) GetAndRemove(key string) (interface{}, bool) {
	s.Lock()
	val, exists := s.M[key]
	if exists {
		delete(s.M, key)
	}
	s.Unlock()
	return val, exists
}

func (s *SafeMap) Clear() {
	s.Lock()
	s.M = make(map[string]interface{})
	s.Unlock()
}

func (s *SafeMap) Keys() []string {
	s.RLock()
	defer s.RUnlock()

	keys := make([]string, 0)
	for key, _ := range s.M {
		keys = append(keys, key)
	}
	return keys
}

func (s *SafeMap) Slice() []interface{} {
	s.RLock()
	defer s.RUnlock()

	valS := make([]interface{}, 0)
	for _, val := range s.M {
		valS = append(valS, val)
	}
	return valS
}

func (s *SafeMap) ContainsKey(key string) bool {
	s.RLock()
	_, exists := s.M[key]
	s.RUnlock()
	return exists
}

func (s *SafeMap) Size() int {
	s.RLock()
	l := len(s.M)
	s.RUnlock()
	return l
}

func (s *SafeMap) IsEmpty() bool {
	s.RLock()
	empty := len(s.M) == 0
	s.RUnlock()
	return empty
}
