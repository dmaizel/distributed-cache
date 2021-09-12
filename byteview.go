package cache

// A ByteView holds an immutable view of bytes.
// The byte type is chosen to support the storage of arbitrary data types, such as character strings, pictures, etc.
type ByteView struct {
	b []byte
}

func (v ByteView) Len() int {
	return len(v.b)
}

// ByteSlice returns a copy of the data as a byte slice.
func (v ByteView) ByteSlice() []byte {
	return cloneBytes(v.b)
}

func (v ByteView) String() string {
	return string(v.b)
}

func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(b, c)
	return c
}
