package main

import "sync"

var handlers = map[string]func([]Data) Data{
	"PING": ping,
	"SET":  set,
	"GET":  get,
}

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
