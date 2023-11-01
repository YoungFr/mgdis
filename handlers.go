package main

import "sync"

// 命令对应的处理函数
var handlers = map[string]func([]Data) Data{
	"PING":  ping,
	"SET":   set,
	"GET":   get,
	"FLUSH": flush,
}

// 是否为写入类型命令
var writingCommands = map[string]bool{
	"SET": true,
}

// PING [message]
func ping(args []Data) Data {
	var reply Data
	switch len(args) {
	case 0:
		{
			reply.dataType = datatypes[SIMPLE_STRING]
			reply.simpleStr = "PONG"
		}
	case 1:
		{
			reply.dataType = datatypes[BULK_STRING]
			reply.bulkStr = args[0].bulkStr
		}
	default:
		{
			reply.dataType = datatypes[ERROR]
			reply.errorMsg = "ERR wrong number of arguments for 'ping' command"
		}
	}
	return reply
}

type Pairs struct {
	kv map[string]string
	mu sync.RWMutex
}

var p = Pairs{
	kv: make(map[string]string),
	mu: sync.RWMutex{},
}

// SET key value
func set(args []Data) Data {
	var reply Data
	switch len(args) {
	case 2:
		{
			k, v := args[0].bulkStr, args[1].bulkStr
			p.mu.Lock()
			p.kv[k] = v
			p.mu.Unlock()
			reply.dataType = datatypes[SIMPLE_STRING]
			reply.simpleStr = "OK"
		}
	default:
		{
			reply.dataType = datatypes[ERROR]
			reply.errorMsg = "ERR wrong number of arguments for 'set' command"
		}
	}
	return reply
}

// GET key
func get(args []Data) Data {
	var reply Data
	switch len(args) {
	case 1:
		{
			k := args[0].bulkStr
			p.mu.Lock()
			v, ok := p.kv[k]
			p.mu.Unlock()
			if ok {
				reply.dataType = datatypes[BULK_STRING]
				reply.bulkStr = v
			} else {
				reply.dataType = datatypes[BULK_STRING]
				reply.isNullBulk = true
			}
		}
	default:
		{
			reply.dataType = datatypes[ERROR]
			reply.errorMsg = "ERR wrong number of arguments for 'get' command"
		}
	}
	return reply
}

// FLUSH
func flush(args []Data) Data {
	var reply Data
	switch len(args) {
	case 0:
		{
			p.mu.Lock()
			for k := range p.kv {
				delete(p.kv, k)
			}
			p.kv = nil
			p.kv = make(map[string]string)
			p.mu.Unlock()
			reply.dataType = datatypes[SIMPLE_STRING]
			reply.simpleStr = "OK"
		}
	default:
		{
			reply.dataType = datatypes[ERROR]
			reply.errorMsg = "ERR wrong number of arguments for 'flush' command"
		}
	}
	return reply
}
