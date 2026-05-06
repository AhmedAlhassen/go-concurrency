package main

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

var (
	host = "ws://localhost"
)

type TestConfig struct {
	clientCount int
	wg          *sync.WaitGroup
}

func DialServer(wg *sync.WaitGroup) {
	defer wg.Done()
	// client
	dialer := websocket.DefaultDialer

	conn, _, err := dialer.Dial(fmt.Sprintf("%s%s", host, WSPort), nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("connected to the server", conn.LocalAddr().String())

	time.Sleep(1 * time.Second)
	conn.Close()
}

func Test_Connection(t *testing.T) {
	// s := NewServer()
	go createWSServer()
	tc := TestConfig{
		clientCount: 10,
		wg:          new(sync.WaitGroup),
	}
	tc.wg.Add(tc.clientCount)

	for range tc.clientCount {
		go DialServer(tc.wg)
	}

	tc.wg.Wait()
	fmt.Println("Exiting test....")
}
