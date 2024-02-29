package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
)

var port = flag.Int("port", 6379, "the port to serve on")

func main() {
	flag.Parse()

	// 服务器地址
	ip := "127.0.0.1"
	addr := fmt.Sprintf(ip+":"+"%d", *port)

	// 根据 AOF 日志恢复数据
	AOFPath = "db_" + ip + "_" + strconv.Itoa(*port) + ".aof" // db_127.0.0.1_6379.aof
	aof, err := NewAOF(AOFPath)
	if err != nil {
		log.Fatalf("failed to create/open aof file: %v\n", err)
	}
	defer aof.Close()
	commands, err := aof.Read()
	if err != nil {
		log.Fatalf("failed to read commands from aof file: %v\n", err)
	}
	for _, command := range commands {
		handlers[strings.ToUpper(command.array[0].bulkStr)](command.array[1:])
	}

	// 在 port 端口开启监听
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v\n", err)
	}
	log.Printf("server starting on port %d...\n", *port)

	// 处理网络 I/O 和命令执行
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Fatalf("failed to accept: %v\n", err)
		}
		go handleConn(conn, aof)
	}
}

func handleConn(conn net.Conn, aof *AOF) {
	defer conn.Close()

	log.Println(conn.RemoteAddr().String() + " connected")

	r := NewRESP(conn)
	for {
		// 接收请求
		req, err := r.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("failed to read client socket: %v\n", err)
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
				// 执行命令
				reply = handler(args)

				// 如果是写入命令则在执行后将命令写入 aof 文件
				if writingCommands[commandName] {
					aof.Write(req)
				}

				// 如果是 FLUSH 命令则已经清空了内存中的数据
				// 还需要清空 aof 文件
				if commandName == "FLUSH" {
					aof.mu.Lock()
					aof.file.Truncate(0)
					aof.file.Sync()
					aof.file.Seek(0, io.SeekStart)
					aof.mu.Unlock()
				}
			} else {
				// 非法命令
				reply.dataType = datatypes[ERROR]
				reply.errorMsg = fmt.Sprintf(
					"ERR unknown command '%s', with args beginning with:%s",
					req.array[0].bulkStr,
					argsOfUnknownCommand(args))
			}
		}

		// 发回响应
		if err := r.Write(reply); err != nil {
			log.Fatalf("failed to write client socket: %v\n", err)
		}
	}

	log.Println(conn.RemoteAddr().String() + " disconnected")
}
