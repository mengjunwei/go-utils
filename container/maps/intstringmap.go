package maps

import (
	"sync"
)

type IntStringMap struct {
	sync.RWMutex
	M map[int]string
}

func NewIntStringMap() *IntStringMap {
	return &IntStringMap{M: make(map[int]string)}
}

func (i *IntStringMap) Clone() map[int]string {
	m := make(map[int]string)
	i.RLock()
	defer i.RUnlock()
	for k, v := range i.M {
		m[k] = v
	}
	return m
}

func (i *IntStringMap) Put(key int, val string) {
	i.Lock()
	defer i.Unlock()
	i.M[key] = val
}

func (i *IntStringMap) Puts(m map[int]string) {
	todo := make(map[int]string)
	i.RLock()
	for k, v := range m {
		old, exists := i.M[k]
		if exists && v == old {
			continue
		}
		todo[k] = v
	}
	i.RUnlock()

	if len(todo) == 0 {
		return
	}

	i.Lock()
	for k, v := range todo {
		i.M[k] = v
	}
	i.Unlock()
}

func (i *IntStringMap) Get(key int) (string, bool) {
	i.RLock()
	defer i.RUnlock()
	val, exists := i.M[key]
	return val, exists
}

func (i *IntStringMap) Exists(key int) bool {
	_, exists := i.Get(key)
	return exists
}

func (i *IntStringMap) Remove(key int) {
	i.Lock()
	defer i.Unlock()
	delete(i.M, key)
}

func (i *IntStringMap) RemoveBatch(keys []int) {
	i.Lock()
	defer i.Unlock()
	for _, key := range keys {
		delete(i.M, key)
	}
}
