package main

import (
	"fmt"
	"strings"
)

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
	ans := strings.Join(ss, " ")
	if ans == "" {
		return ans
	}
	return " " + ans
}
