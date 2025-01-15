package buffer

import (
	"fmt"
	"os"
)

const (
	BP_RDONLY int = os.O_RDONLY
	BP_WRONLY     = os.O_WRONLY
	BP_RDWR       = os.O_RDWR
	BP_APPEND     = os.O_APPEND
	BP_CREATE     = os.O_CREATE
)

type FS interface {
	Open(filePath string, flags, mode int) (FS, error)
	Read(buf []byte, offset int64) (int, error)
	Write(buf []byte, offset int64) (int, error)
	Sync() error
	Close() error
}

type LocalFS struct {
	file *os.File
}

func (lfs *LocalFS) Open(filePath string, flags, mode int) (FS, error) {
	if lfs.file != nil {
		return lfs, nil
	}

	file, err := os.OpenFile(filePath, flags, os.FileMode(mode))
	if err != nil {
		return nil, fmt.Errorf("Open: %v", err)
	}
	lfs.file = file

	return lfs, nil
}

func (lfs *LocalFS) Read(buf []byte, offset int64) (int, error) {
	return lfs.file.ReadAt(buf, offset)
}

func (lfs *LocalFS) Write(buf []byte, offset int64) (int, error) {
	return lfs.file.WriteAt(buf, offset)
}

func (lfs *LocalFS) Sync() error {
	return lfs.file.Sync()
}

func (lfs *LocalFS) Close() error {
	return lfs.file.Close()
}
