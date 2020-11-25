package bytes_util

import (
	"reflect"
	"unsafe"
)

// 字节切片进行cap扩容
func Resize(b []byte, n int) []byte {
	if nn := n - cap(b); nn > 0 {
		b = append(b[:cap(b)], make([]byte, nn)...)
	}
	return b[:n]
}

// 字节切片转字符串
func ToUnsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// 字符串转字节切片
func ToUnsafeBytes(s string) (b []byte) {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	slh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	slh.Data = sh.Data
	slh.Len = sh.Len
	slh.Cap = sh.Len
	return b
}
