package main

import (
	"io"
	"os"
	"sync"
	"time"
)

type AOF struct {
	file *os.File
	mu   sync.Mutex
}

func NewAOF(path string) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	aof := &AOF{
		file: f,
		mu:   sync.Mutex{},
	}
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

func (a *AOF) read() ([]Data, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.file.Seek(0, io.SeekStart)
	// 读出文件中所有的命令
	ds := make([]Data, 0)
	r := NewRESP(a.file)
	for {
		d, err := r.read()
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

func (a *AOF) write(req Data) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, err := a.file.Write(req.marshal())
	if err != nil {
		return err
	}
	return nil
}

func (a *AOF) close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.file.Close()
}
