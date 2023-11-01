package main

import (
	"bufio"
	"errors"
	"io"
	"strconv"
)

// 在 RESP2 协议中定义的五种数据类型
const (
	SIMPLE_STRING = '+' // 简单字符串
	ERROR         = '-' // 错误消息
	INTEGER       = ':' // 整数
	BULK_STRING   = '$' // 多行字符串
	ARRAY         = '*' // 数组
)

var datatypes = map[byte]string{
	SIMPLE_STRING: "SIMPLE_STRING",
	ERROR:         "ERROR",
	INTEGER:       "INTEGER",
	BULK_STRING:   "BULK_STRING",
	ARRAY:         "ARRAY",
}

// 客户端的命令和服务器的执行结果都使用 Data 结构体来表示
// 字段 dataType 的值是 "SIMPLE_STRING"、"ERROR"、"INTEGER"、"BULK_STRING" 和 "ARRAY" 之一
// 剩余的字段根据 dataType 的值来相应地设置
//
// 客户端的命令 (命令名 + 参数) 总是被编码成一个多行字符串的数组
// 所以在解析客户端发来的消息时只需要处理 BULK_STRING 和 ARRAY 类型
//
// 服务器的执行结果根据命令的不同可以是任何格式
type Data struct {
	dataType    string
	simpleStr   string
	errorMsg    string
	integer     int64
	bulkStr     string
	isNullBulk  bool
	array       []Data
	isNullArray bool
}

// 将一个 Data 结构体根据 RESP 协议的编码格式转换成字节数组
func (d Data) marshal() []byte {
	switch d.dataType {
	case "SIMPLE_STRING":
		{
			return d.marshalSimpleString()
		}
	case "ERROR":
		{
			return d.marshalError()
		}
	case "INTEGER":
		{
			return d.marshalInteger()
		}
	case "BULK_STRING":
		{
			return d.marshalBulk()
		}
	case "ARRAY":
		{
			return d.marshalArray()
		}
	default:
		{
			return make([]byte, 0)
		}
	}
}

func (d Data) marshalSimpleString() []byte {
	bytes := make([]byte, 0)
	// +
	bytes = append(bytes, SIMPLE_STRING)
	// <data>
	bytes = append(bytes, []byte(d.simpleStr)...)
	// \r\n
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (d Data) marshalError() []byte {
	bytes := make([]byte, 0)
	// -
	bytes = append(bytes, ERROR)
	// <error-message>
	bytes = append(bytes, []byte(d.errorMsg)...)
	// \r\n
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (d Data) marshalInteger() []byte {
	bytes := make([]byte, 0)
	// :
	bytes = append(bytes, INTEGER)
	// <value>
	bytes = append(bytes, []byte(strconv.FormatInt(d.integer, 10))...)
	// \r\n
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (d Data) marshalBulk() []byte {
	// 多行字符串的长度为  0 时表示空字符串 ("")
	// 多行字符串的长度为 -1 时表示一个空值 (nil)
	if d.isNullBulk {
		return []byte("$-1\r\n")
	}
	bytes := make([]byte, 0)
	// $
	bytes = append(bytes, BULK_STRING)
	// <length>
	bytes = append(bytes, []byte(strconv.Itoa(len(d.bulkStr)))...)
	// \r\n
	bytes = append(bytes, '\r', '\n')
	// <data>
	bytes = append(bytes, []byte(d.bulkStr)...)
	// \r\n
	bytes = append(bytes, '\r', '\n')
	return bytes
}

func (d Data) marshalArray() []byte {
	// 长度为 -1 的数组表示数组空值
	if d.isNullArray {
		return []byte("*-1\r\n")
	}
	bytes := make([]byte, 0)
	// *
	bytes = append(bytes, ARRAY)
	// <number-of-elements>
	bytes = append(bytes, []byte(strconv.Itoa(len(d.array)))...)
	// \r\n
	bytes = append(bytes, '\r', '\n')
	// <element-1>...<element-n>
	for i := 0; i < len(d.array); i++ {
		bytes = append(bytes, d.array[i].marshal()...)
	}
	return bytes
}

// 序列化与反序列化
type RESP struct {
	reader *bufio.Reader
	writer *bufio.Writer
}

func NewRESP(rwc io.ReadWriteCloser) *RESP {
	return &RESP{
		reader: bufio.NewReader(rwc),
		writer: bufio.NewWriter(rwc),
	}
}

// 序列化：GO-STRUCT => RESP
func (r *RESP) write(d Data) error {
	if _, err := r.writer.Write(d.marshal()); err != nil {
		return err
	}
	// Don't forget to flush!
	if err := r.writer.Flush(); err != nil {
		return err
	}
	return nil
}

// 反序列化：RESP => GO-STRUCT
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
	return data, errUnknownRequestDataType
}

var errUnknownRequestDataType = errors.New("unknown request data type")

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
// 这个函数用于读取多行字符串的 <length>\r\n 部分和数组的 <number-of-elements>\r\n 部分
func (r *RESP) readInteger() (x int64, n int, err error) {
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

	// 使用 readInteger 函数读入 <length>\r\n 部分，得到多行字符串的长度
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

	// 使用 readInteger 函数读入 <number-of-elements>\r\n 部分，得到数组的长度
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
