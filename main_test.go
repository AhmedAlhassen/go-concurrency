package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

var (
	host = "ws://localhost"
)

type TestConfig struct {
	clientCount    int
	wg             *sync.WaitGroup
	brMsgCount     *atomic.Int64
	targetMsgCount int
}

type TestClient struct {
	conn  *websocket.Conn
	msgCH chan *ReqMsg
	ctx   context.Context
}

func NewTestClien(conn *websocket.Conn, ctx context.Context) *TestClient {
	return &TestClient{
		conn:  conn,
		msgCH: make(chan *ReqMsg, 64),
		ctx:   ctx,
	}
}

func (c *TestClient) writeLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case msg := <-c.msgCH:
			if err := c.conn.WriteJSON(msg); err != nil {
				fmt.Printf("error sending msg %v\n", err)
				return
			}
		}
	}
}

func DialServer(tc *TestConfig) *websocket.Conn {
	exit := make(chan struct{})
	// client
	dialer := websocket.DefaultDialer

	conn, _, err := dialer.Dial(fmt.Sprintf("%s%s", host, WSPort), nil)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		for {
			time.Sleep(2 * time.Second)
			if tc.targetMsgCount == int(tc.brMsgCount.Load()) {
				close(exit)
				return
			}
		}
	}()

	go func() {
		<-exit
		conn.Close()
		tc.wg.Done()
	}()
	// time.Sleep(2 * time.Second)

	go func() {
		for {
			_, b, err := conn.ReadMessage()
			if err != nil {
				return
			}

			if len(b) > 0 {
				tc.brMsgCount.Add(1)
			}
		}
	}()
	return conn
}

func Test_Connection(t *testing.T) {
	// s := NewServer()
	go createWSServer()
	ctx, cancel := context.WithCancel(context.Background())

	time.Sleep(1 * time.Second)
	clientCount := 500
	brCount := 100

	tc := TestConfig{
		clientCount:    clientCount,
		wg:             new(sync.WaitGroup),
		brMsgCount:     new(atomic.Int64),
		targetMsgCount: clientCount * brCount,
	}
	tc.wg.Add(tc.clientCount + 1)

	brConn := DialServer(&tc)
	brClient := NewTestClien(brConn, ctx)
	go brClient.writeLoop()

	for range tc.clientCount {
		go DialServer(&tc)
	}

	time.Sleep(1 * time.Second)

	for range brCount {
		msg := NewReqMsg()
		msg.MsgType = MsgType_Broadcast
		msg.Data = "hello from tests"
		brClient.msgCH <- msg
	}

	tc.wg.Wait()
	cancel()

	fmt.Println("Exiting test....")
}
