package buffer

import (
	"context"
	"log"
	"os"
	"path"
	"runtime/pprof"
	"testing"
)

func BenchmarkWrite(b *testing.B) {
	prof_file, err := os.Create("../profiles/profile.prof")
	if err != nil {
		log.Fatal("Unable to create profile flag")
	}
	defer prof_file.Close()

	baseDir := b.TempDir()
	perm := 0777

	pprof.StartCPUProfile(prof_file)
	defer pprof.StopCPUProfile()

	os.MkdirAll(path.Join(baseDir, "1000/1001"), os.FileMode(perm))

	_, err = os.OpenFile(path.Join(baseDir, "1000/1001/1004"), os.O_CREATE, os.FileMode(perm))
	if err != nil {
		log.Fatalf("OpenFile: %v", err)
	}

	timeout := 25
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	buf := NewBuffer(10, 1024*1024, baseDir, int32(perm), 2, &LocalFS{})
	if buf == nil {
		log.Fatal("NewBuffer: Unable to create buffer pool")
	}
	defer buf.Close()

	go buf.Run(ctx)

	data := []byte("Hello World")
	key := NewKey(1000, 1001, 1004)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf.PutRecord(ctx, key, &data, int32(timeout), false)
	}
}
