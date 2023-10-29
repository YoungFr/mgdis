package main

import (
	"bufio"
	"errors"
	"io"
	"net"
	"strconv"
)

// 在 RESP2 协议中定义的数据类型有以下五种
// 由于客户端总是将命令和参数编码成一个多行字符串的数组
// 所以在解析客户端发来的消息时只需要处理 BULK_STRING 和 ARRAY 类型
const (
	SIMPLE_STRING = '+' // 简单字符串
	ERROR         = '-' // 错误消息
	INTEGER       = ':' // 整数
	BULK_STRING   = '$' // 多行字符串
	ARRAY         = '*' // 数组
)

var datatypes = map[byte]string{
	'+': "SIMPLE_STRING",
	'-': "ERROR",
	':': "INTEGER",
	'$': "BULK_STRING",
	'*': "ARRAY",
}

// 字段 dataType 的值是字符串 SIMPLE_STRING、ERROR、INTEGER、BULK_STRING 和 ARRAY 之一
// 剩余的字段根据 dataType 的值来相应地设置 (目前只定义了 BULK_STRING 和 ARRAY 类型对应的字段)
type Data struct {
	dataType string
	bulkStr  string
	array    []Data
}

type RESP struct {
	reader *bufio.Reader
}

func NewRESP(conn net.Conn) *RESP {
	return &RESP{
		reader: bufio.NewReader(conn),
	}
}

// 循环读入字节直到遇到 '\r' 和 '\n' 字符为止
func (r *RESP) readLine() (line []byte, n int, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n++
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' && line[len(line)-1] == '\n' {
			break
		}
	}
	// 丢掉最后的 '\r' 和 '\n' 字符
	return line[:len(line)-2], n, nil
}

// 读入整数的值
// :[<+|->]<value>\r\n
func (r *RESP) readInteger() (x int64, n int, err error) {
	// 使用 readLine 函数读入 [<+|->]<value> 部分
	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err
	}
	x, err = strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}
	return x, n, nil
}

// 读入多行字符串
// $<length>\r\n<data>\r\n
func (r *RESP) readBulk() (data Data, err error) {
	data.dataType = datatypes[BULK_STRING]

	// 使用 readInteger 函数读入 <length> 部分，得到多行字符串的长度
	length, _, err := r.readInteger()
	if err != nil {
		return data, err
	}

	// 读入 <data> 部分，也就是读入恰好 length 个字节到 bulk 中
	bulk := make([]byte, length)
	if _, err = io.ReadFull(r.reader, bulk); err != nil {
		return data, err
	}

	// 读入最后的 '\r' 和 '\n' 字符
	if _, err = r.reader.ReadByte(); err != nil {
		return data, err
	}
	if _, err = r.reader.ReadByte(); err != nil {
		return data, err
	}

	data.bulkStr = string(bulk)
	return data, nil
}

// 使用递归下降法读取数组
// *<number-of-elements>\r\n<element-1>...<element-n>
func (r *RESP) readArray() (data Data, err error) {
	data.dataType = datatypes[ARRAY]

	// 使用 readInteger 函数读入 <number-of-elements> 部分，得到数组的长度
	arrlen, _, err := r.readInteger()
	if err != nil {
		return data, err
	}

	// 依次读入数组中的各个元素
	data.array = make([]Data, 0)
	var i int64
	for i < arrlen {
		d, err := r.read()
		if err != nil {
			return data, err
		}
		data.array = append(data.array, d)
		i++
	}

	return data, nil
}

var errUnknownClientDataType = errors.New("unknown client data type")

func (r *RESP) read() (data Data, err error) {
	dataType, err := r.reader.ReadByte()
	if err != nil {
		return data, err
	}
	switch dataType {
	case BULK_STRING:
		{
			return r.readBulk()
		}
	case ARRAY:
		{
			return r.readArray()
		}
	}
	return data, errUnknownClientDataType
}
