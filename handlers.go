package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

// 命令对应的处理函数
var handlers = map[string]func([]Data) Data{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"FLUSH":   flush,
	"DEL":     del,
	"PEXPIRE": pexpire,
	"PTTL":    pttl,
}

// 是否为写入类型的命令
var writingCommands = map[string]bool{
	"SET": true,
	"DEL": true,
}

func wrongNumArgsErrMsgFormatter(cmd string) string {
	return fmt.Sprintf("ERR wrong number of arguments for '%s' command", cmd)
}

// PING [message]
func ping(args []Data) Data {
	var reply Data
	switch len(args) {
	case 0:
		// 参数个数为 0 时返回 "PONG" 简单字符串
		reply.dataType = datatypes[SIMPLE_STRING]
		reply.simpleStr = "PONG"
	case 1:
		// 参数个数为 1 时返回用户输入的多行字符串
		reply.dataType = datatypes[BULK_STRING]
		reply.bulkStr = args[0].bulkStr
	default:
		reply.dataType = datatypes[ERROR]
		reply.errorMsg = wrongNumArgsErrMsgFormatter("ping")
	}
	return reply
}

// 存储所有键值对的字典
var p sync.Map

// SET key value
func set(args []Data) Data {
	var reply Data
	switch len(args) {
	case 2:
		// 参数个数为 2 时设置键值对并返回 "OK" 简单字符串
		k, v := args[0].bulkStr, args[1].bulkStr
		p.Store(k, v)
		reply.dataType = datatypes[SIMPLE_STRING]
		reply.simpleStr = "OK"
	default:
		reply.dataType = datatypes[ERROR]
		reply.errorMsg = wrongNumArgsErrMsgFormatter("set")
	}
	return reply
}

// GET key
func get(args []Data) Data {
	var reply Data
	switch len(args) {
	case 1:
		// 参数个数为 1 时返回键 key 对应的值（如果存在）或空值（如果不存在）
		reply.dataType = datatypes[BULK_STRING]

		// 获取键 key 的过期时间
		k := args[0].bulkStr
		expireTime, hit := expires.Load(k)

		if hit && time.Now().UnixMilli() > expireTime.(int64) {
			// 如果设置了过期时间且已过期返回空值
			reply.isNullBulk = true
			// 惰性删除
			p.Delete(k)
			expires.Delete(k)
		} else {
			// 其他情况下都是正常从哈希表中读取值
			if v, ok := p.Load(k); ok {
				reply.bulkStr = v.(string)
			} else {
				reply.isNullBulk = true
			}
		}
	default:
		reply.dataType = datatypes[ERROR]
		reply.errorMsg = wrongNumArgsErrMsgFormatter("get")
	}
	return reply
}

// FLUSH
func flush(args []Data) Data {
	var reply Data
	switch len(args) {
	case 0:
		// 参数个数为 0 时删除所有的键并返回 "OK" 简单字符串
		p.Range(func(key, value any) bool {
			p.Delete(key)
			return true
		})
		reply.dataType = datatypes[SIMPLE_STRING]
		reply.simpleStr = "OK"
	default:
		reply.dataType = datatypes[ERROR]
		reply.errorMsg = wrongNumArgsErrMsgFormatter("flush")
	}
	return reply
}

// DEL key [key ...]
func del(args []Data) Data {
	var reply Data
	switch len(args) {
	case 0:
		// 参数个数为 0 时是非法的
		reply.dataType = datatypes[ERROR]
		reply.errorMsg = wrongNumArgsErrMsgFormatter("del")
	default:
		// 参数个数大于等于 1 时返回成功删除的键的个数

		// 客户端提供的要删除的键的集合
		has := make(map[string]bool)
		for _, arg := range args {
			has[arg.bulkStr] = true
		}
		cnt := int64(0)
		p.Range(func(key, value any) bool {
			if has[key.(string)] {
				p.Delete(key)
				cnt++
			}
			return true
		})

		reply.dataType = datatypes[INTEGER]
		reply.integer = cnt
	}
	return reply
}

// 过期字典
var expires sync.Map

// PEXPIRE key milliseconds
func pexpire(args []Data) Data {
	var reply Data
	switch len(args) {
	case 2:
		k := args[0].bulkStr                    // 键
		e, err := strconv.Atoi(args[1].bulkStr) // 键在 e 毫秒后过期
		if err != nil {
			reply.dataType = datatypes[ERROR]
			reply.errorMsg = "ERR value is not an integer or out of range"
		} else {
			reply.dataType = datatypes[INTEGER]
			if _, ok := p.Load(k); ok {
				// 成功设置时返回 1
				expireTime := time.Now().Add(time.Duration(e * 1e6)).UnixMilli()
				expires.Store(k, expireTime)
				reply.integer = 1
			} else {
				// 键不存在时返回 0
				reply.integer = 0
			}
		}
	default:
		reply.dataType = datatypes[ERROR]
		reply.errorMsg = wrongNumArgsErrMsgFormatter("pexpire")
	}
	return reply
}

// PTTL key
func pttl(args []Data) Data {
	var reply Data
	switch len(args) {
	case 1:
		k := args[0].bulkStr
		reply.dataType = datatypes[INTEGER]

		if _, ok := p.Load(k); ok {
			// 键存在
			if expireTime, ok := expires.Load(k); ok {
				ttl := expireTime.(int64) - time.Now().UnixMilli()
				if ttl <= 0 {
					// 设置了过期时间且已过期
					p.Delete(k)
					expires.Delete(k)
					reply.integer = -2
				} else {
					// 设置了过期时间但未过期
					reply.integer = ttl
				}
			} else {
				// 未设置过期时间
				reply.integer = -1
			}
		} else {
			// 键不存在
			reply.integer = -2
		}
	default:
		reply.dataType = datatypes[ERROR]
		reply.errorMsg = wrongNumArgsErrMsgFormatter("pttl")
	}
	return reply
}
