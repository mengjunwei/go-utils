package set

import (
	"fmt"
	"sync"
)

type SafeInt64Set struct {
	sync.RWMutex
	M map[int64]struct{}
}

func NewSafeInt64Set() *SafeInt64Set {
	return &SafeInt64Set{M: make(map[int64]struct{})}
}

func (si *SafeInt64Set) String() string {
	s := si.Slice()
	return fmt.Sprint(s)
}

func (si *SafeInt64Set) Add(item int64) *SafeInt64Set {
	if si.Contains(item) {
		return si
	}

	si.Lock()
	si.M[item] = struct{}{}
	si.Unlock()
	return si
}

func (si *SafeInt64Set) Contains(item int64) bool {
	si.RLock()
	_, exists := si.M[item]
	si.RUnlock()
	return exists
}

func (si *SafeInt64Set) Adds(items []int64) *SafeInt64Set {
	count := len(items)
	if count == 0 {
		return si
	}

	todo := make([]int64, 0, count)
	si.RLock()
	for i := 0; i < count; i++ {
		_, exists := si.M[items[i]]
		if exists {
			continue
		}

		todo = append(todo, items[i])
	}
	si.RUnlock()

	count = len(todo)
	if count == 0 {
		return si
	}

	si.Lock()
	for i := 0; i < count; i++ {
		si.M[todo[i]] = struct{}{}
	}
	si.Unlock()
	return si
}

func (si *SafeInt64Set) Size() int {
	si.RLock()
	l := len(si.M)
	si.RUnlock()
	return l
}

func (si *SafeInt64Set) Clear() {
	si.Lock()
	si.M = make(map[int64]struct{})
	si.Unlock()
}

func (si *SafeInt64Set) Slice() []int64 {
	si.RLock()
	ret := make([]int64, len(si.M))
	i := 0
	for item := range si.M {
		ret[i] = item
		i++
	}

	si.RUnlock()
	return ret
}
