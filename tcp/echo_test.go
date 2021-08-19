package tcp

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestListenAndServe(t *testing.T) {
	var err error
	closeChan := make(chan struct{})
	listener, err := net.Listen("tcp", ":0") // a port number is automatically chosen
	if err != nil {
		t.Error(err)
		return
	}
	addr := listener.Addr().String()
	go ListenAndServe(listener, MakeEchoHandler(), closeChan)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
		return
	}
	for i := 0; i < 10; i++ {
		val := strconv.Itoa(rand.Int())
		_, err = conn.Write([]byte(val + "\n"))
		if err != nil {
			t.Error(err)
			return
		}
		bufReader := bufio.NewReader(conn)
		line, _, err := bufReader.ReadLine()
		if err != nil {
			t.Error(err)
			return
		}
		fmt.Printf("Client get it: %s\n", val)
		if string(line) != val {
			t.Error("get wrong response")
			return
		}
	}
	_ = conn.Close()

	for i := 0; i < 5; i++ {
		// create idle connection -> 测试闲置的连接是不是都会被关闭
		conn2, _ := net.Dial("tcp", addr)
		//fmt.Println(conn2.LocalAddr())
		conn2.Write([]byte("why not \n"))
	}
	closeChan <- struct{}{}
	time.Sleep(time.Second)
}