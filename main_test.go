package main

import (
	"fmt"
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

func DialServer(clientNum int, wg *sync.WaitGroup, failedAt chan<- int) {
	defer wg.Done()

	// client
	dialer := websocket.DefaultDialer

	conn, _, err := dialer.Dial(fmt.Sprintf("%s%s", host, WSPort), nil)
	if err != nil {
		select {
		case failedAt <- clientNum:
			fmt.Printf("first failed client was #%d: %v\n", clientNum, err)
		default:
		}
		return
	}

	defer func() {
		conn.Close()
	}()
	fmt.Printf("client #%d connected to the server %s\n", clientNum, conn.LocalAddr().String())

	time.Sleep(2 * time.Second)

}

func Test_Connection(t *testing.T) {
	// s := NewServer()
	go createWSServer()
	time.Sleep(1 * time.Second)

	tc := TestConfig{
		clientCount: 5,
		wg:          new(sync.WaitGroup),
	}
	tc.wg.Add(tc.clientCount)
	failedAt := make(chan int, 1)

	for i := range tc.clientCount {
		go DialServer(i+1, tc.wg, failedAt)
	}

	tc.wg.Wait()
	select {
	case n := <-failedAt:
		t.Fatalf("first client failure happened at client #%d", n)
	default:
	}
	time.Sleep(1 * time.Second)
	fmt.Println("Exiting test....")
}
