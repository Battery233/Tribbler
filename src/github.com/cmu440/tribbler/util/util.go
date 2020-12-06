package util

func CompareUint32Slice(s1, s2 []uint32) bool {
	if s1 == nil || s2 == nil {
		return false
	}
	if len(s1) != len(s2) {
		return false
	} else {
		for i := 0; i < len(s1); i++ {
			if s1[i] != s2[i] {
				return false
			}
		}
	}
	return true
}

type UInt32Sorter []uint32

func (f UInt32Sorter) Len() int {
	return len(f)
}

func (f UInt32Sorter) Less(i, j int) bool {
	return f[i] < f[j]
}

func (f UInt32Sorter) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}
