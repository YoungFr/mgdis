package main

import (
	"io"
	"os"
	"sync"
	"time"
)

type AOF struct {
	mu   sync.Mutex
	file *os.File
}

var AOFPath string

func NewAOF(path string) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	aof := &AOF{file: f}

	// 后台 AOF 刷盘线程
	go func() {
		for {
			aof.mu.Lock()
			aof.file.Sync()
			aof.mu.Unlock()
			time.Sleep(1 * time.Second)
		}
	}()

	return aof, nil
}

func (a *AOF) Read() ([]Data, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.file.Seek(0, io.SeekStart)
	r := NewRESP(a.file)
	ds := make([]Data, 0)

	// 读出 aof 文件中所有的命令
	for {
		d, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		ds = append(ds, d)
	}

	return ds, nil
}

func (a *AOF) Write(req Data) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	_, err := a.file.Write(req.marshal())
	if err != nil {
		return err
	}

	return nil
}

func (a *AOF) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.file.Close()
}
