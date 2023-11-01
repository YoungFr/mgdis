package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"strings"
)

func main() {
	fmt.Println("Listening on port 6379...")
	ln, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Fatalln(err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatalln(err)
		}
		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()
	fmt.Println(conn.RemoteAddr().String() + " connected")
	// 持久化
	aof, err := NewAOF("db.aof")
	if err != nil {
		log.Fatalln(err)
	}
	defer aof.close()
	// 在处理接收到的命令前先将 aof 文件中的命令都执行一遍
	if commands, err := aof.read(); err != nil {
		log.Fatalln(err)
	} else {
		for _, command := range commands {
			_ = handlers[strings.ToUpper(command.array[0].bulkStr)](command.array[1:])
		}
	}
	// 序列化和反序列化
	r := NewRESP(conn)
	for {
		// 接收请求
		req, err := r.read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalln(err)
		}
		var reply Data
		// 处理请求
		if !valid(req) {
			reply.dataType = datatypes[ERROR]
			reply.errorMsg = "ERR invalid command format"
		} else {
			// 数组的第一个元素是命令的名称
			commandName := strings.ToUpper(req.array[0].bulkStr)
			// 后边的元素是命令的参数
			args := req.array[1:]
			if handler, ok := handlers[commandName]; ok {
				// 如果是写入命令 => 在执行前先将命令写入 aof 文件
				if writingCommands[commandName] {
					aof.write(req)
				}
				// 执行命令
				reply = handler(args)
				// 是 FLUSH 命令 => 清空 aof 文件
				if commandName == "FLUSH" {
					aof.mu.Lock()
					aof.file.Truncate(0)
					aof.file.Seek(0, io.SeekStart)
					aof.mu.Unlock()
				}
			} else {
				// 非法命令
				reply.dataType = datatypes[ERROR]
				reply.errorMsg = fmt.Sprintf("ERR unknown command '%s', with args beginning with:%s", req.array[0].bulkStr, argsOfUnknownCommand(args))
			}
		}
		// 发回响应
		if err := r.write(reply); err != nil {
			log.Fatalln(err)
		}
	}
	fmt.Println(conn.RemoteAddr().String() + " disconnected")
}
