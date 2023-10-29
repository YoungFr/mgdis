package main

import (
	"fmt"
	"io"
	"log"
	"net"
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
		data, err := r.read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalln(err)
		}
		fmt.Println(data)
		conn.Write([]byte("+OK\r\n"))
	}
}
