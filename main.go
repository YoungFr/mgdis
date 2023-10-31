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

	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Fatalln(err)
	}

	conn, err := l.Accept()
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()

	r := NewRESP(conn)

	for {
		// 接收命令
		req, err := r.read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalln(err)
		}

		var reply Data
		if !valid(req) {
			reply.dataType = datatypes[ERROR]
			reply.errorMsg = "ERR invalid command format"
		} else {
			// 数组的第一个元素是命令的名称
			command := strings.ToUpper(req.array[0].bulkStr)
			// 后边的元素是命令的参数
			args := req.array[1:]
			if handler, ok := handlers[command]; ok {
				reply = handler(args)
			} else {
				reply.dataType = datatypes[ERROR]
				reply.errorMsg = fmt.Sprintf("ERR unknown command '%s', with args beginning with:%s", req.array[0].bulkStr, argsOfUnknownCommand(args))
			}
		}

		// 发回响应
		if err := r.write(reply); err != nil {
			log.Fatalln(err)
		}
	}
}
