package set

type StringSet struct {
	M map[string]struct{}
}

func NewStringSet() *StringSet {
	return &StringSet{
		M: make(map[string]struct{}),
	}
}

func (ss *StringSet) Add(elt string) *StringSet {
	ss.M[elt] = struct{}{}
	return ss
}

func (ss *StringSet) Exists(elt string) bool {
	_, exists := ss.M[elt]
	return exists
}

func (ss *StringSet) Delete(elt string) {
	delete(ss.M, elt)
}

func (ss *StringSet) Clear() {
	ss.M = make(map[string]struct{})
}

func (ss *StringSet) ToSlice() []string {
	count := len(ss.M)
	if count == 0 {
		return []string{}
	}

	r := make([]string, count)

	i := 0
	for elt := range ss.M {
		r[i] = elt
		i++
	}

	return r
}
