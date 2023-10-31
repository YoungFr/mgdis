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
				reply.errorMsg = fmt.Sprintf("ERR unknown command '%s', with args beginning with: %s", req.array[0].bulkStr, argsOfUnknownCommand(args))
			}
		}

		// 发回响应
		if err := r.write(reply); err != nil {
			log.Fatalln(err)
		}
	}
}

// 检验客户端发来的请求是否是合法的命令格式
// 合法的命令是长度大于等于 1 且只包含多行字符串的数组
func valid(req Data) bool {
	if req.dataType != datatypes[ARRAY] {
		return false
	}
	if req.isNullArray || len(req.array) == 0 {
		return false
	}
	for i := 0; i < len(req.array); i++ {
		if req.array[i].dataType != datatypes[BULK_STRING] {
			return false
		}
	}
	return true
}

func argsOfUnknownCommand(args []Data) string {
	ss := make([]string, 0)
	for _, arg := range args {
		ss = append(ss, fmt.Sprintf("'%s'", arg.bulkStr))
	}
	return strings.Join(ss, " ")
}
