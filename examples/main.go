package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"

	bp "github.com/misachi/bufferpool/buffer"
)

func main() {
	//=========================================================================
	/* Setup to be used for file locations */
	os.MkdirAll(".tmp/data/1000/1001", 0600)

	_, err := os.OpenFile(".tmp/data/1000/1001/1004", os.O_CREATE, 0600)
	if err != nil {
		log.Fatalf("OpenFile: %v", err)
	}
	//=========================================================================

	timeout := 10
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	buf := bp.NewBuffer(2, 1024*1024, ".tmp/data/", 0600, 2, &bp.LocalFS{})
	if buf == nil {
		log.Fatal("NewBuffer: Unable to create buffer pool")
	}
	defer buf.Close()

	go buf.Run(ctx)

	key := bp.NewKey(1000, 1001, 1004)

	data := []byte("Hello World")
	buf.PutRecord(ctx, key, &data, int32(timeout), false)

	data = []byte("Hello World2")
	buf.PutRecord(ctx, key, &data, int32(timeout), false)

	data = []byte("Hello World3")
	buf.PutRecord(ctx, key, &data, int32(timeout), false)

	data = []byte("Hello World4")
	buf.PutRecord(ctx, key, &data, int32(timeout), false)

	data = []byte("Hello World5")
	buf.PutRecord(ctx, key, &data, int32(timeout), false)

	page, err := buf.GetPage(ctx, key, int32(timeout), bp.EXCLUSIVE)
	if err != nil {
		log.Fatalf("GetPage: %v", err)
	}
	defer page.Unlock()

	ret := page.GetRecord(&data, func(key3, d0 *[]byte) bool {
		return bytes.Equal(*key3, *d0)
	})

	if ret == nil {
		log.Fatal("Unable to get record")
	}
	fmt.Printf("Result: %s\n", *ret)
}
