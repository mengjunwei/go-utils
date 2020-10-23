package set

type IntSet struct {
	M map[int]struct{}
}

func NewIntSet() *IntSet {
	return &IntSet{
		M: make(map[int]struct{}),
	}
}

func (i *IntSet) Add(elt int) *IntSet {
	i.M[elt] = struct{}{}
	return i
}

func (i *IntSet) Exists(elt int) bool {
	_, exists := i.M[elt]
	return exists
}

func (i *IntSet) Delete(elt int) {
	delete(i.M, elt)
}

func (i *IntSet) Clear() {
	i.M = make(map[int]struct{})
}

func (i *IntSet) ToSlice() []int {
	count := len(i.M)
	if count == 0 {
		return []int{}
	}

	r := make([]int, count)

	j := 0
	for elt := range i.M {
		r[j] = elt
		j++
	}

	return r
}
