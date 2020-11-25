package set

import (
	"sync"
)

type SafeSet struct {
	sync.RWMutex
	M map[string]bool
}

func NewSafeSet() *SafeSet {
	return &SafeSet{
		M: make(map[string]bool),
	}
}

func (ss *SafeSet) Add(key string) {
	ss.Lock()
	ss.M[key] = true
	ss.Unlock()
}

func (ss *SafeSet) Remove(key string) {
	ss.Lock()
	delete(ss.M, key)
	ss.Unlock()
}

func (ss *SafeSet) Clear() {
	ss.Lock()
	ss.M = make(map[string]bool)
	ss.Unlock()
}

func (ss *SafeSet) Contains(key string) bool {
	ss.RLock()
	_, exists := ss.M[key]
	ss.RUnlock()
	return exists
}

func (ss *SafeSet) Size() int {
	ss.RLock()
	len := len(ss.M)
	ss.RUnlock()
	return len
}

func (ss *SafeSet) ToSlice() []string {
	ss.RLock()
	defer ss.RUnlock()

	count := len(ss.M)
	if count == 0 {
		return []string{}
	}

	r := []string{}
	for key := range ss.M {
		r = append(r, key)
	}

	return r
}
