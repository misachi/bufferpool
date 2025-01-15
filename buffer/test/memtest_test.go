package main

// Testing two simple implementations of zeroing a slice:
// one uses `copy` while another uses a loop. For smaller
// slices the `copy` version performs better

import (
	"testing"

	bp "github.com/misachi/bufferpool/buffer"
)

var arr = [bp.PAGESIZE]byte{0}

//go:noinline
func arrCopy(data *[]byte) {
	copy(*data, arr[:])
}

//go:noinline
func arrLoop(data *[]byte) {
	for i := 0; i < bp.PAGESIZE; i++ {
		(*data)[i] = '0'
	}
}

func BenchmarkArrCopy(b *testing.B) {
	data := make([]byte, bp.PAGESIZE)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		arrCopy(&data)
	}
}

func BenchmarkArrLoop(b *testing.B) {
	data := make([]byte, bp.PAGESIZE)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		arrLoop(&data)
	}
}
