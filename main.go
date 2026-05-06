package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var (
	WSPort = ":3223"
)

type MsgType string

const (
	MsgType_Broadcast MsgType = "broadcast"
)

type ReqMsg struct {
	MsgType MsgType
	client  *Client
	data    string
}

type Client struct {
	ID   string
	mu   *sync.RWMutex
	conn *websocket.Conn
}

func NewClient(conn *websocket.Conn) *Client {
	ID := rand.Text()[:9]

	return &Client{
		ID:   ID,
		mu:   new(sync.RWMutex),
		conn: conn,
	}
}

func (c *Client) readMsgLoop(srv *Server) {
	defer func() {
		c.conn.Close()
		srv.leaveServerCH <- c
	}()

	for {
		_, b, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		msg := new(ReqMsg)
		err = json.Unmarshal(b, msg)
		if err != nil {
			fmt.Printf("unable to unmarshal the msg %v\n", err)
			continue
		}
		srv.broadcastCH <- msg
	}
}

type Server struct {
	clients       map[string]Client
	mu            *sync.RWMutex
	joinServerCH  chan *Client
	leaveServerCH chan *Client
	broadcastCH   chan *ReqMsg
}

func NewServer() *Server {
	return &Server{
		clients:       map[string]Client{},
		mu:            new(sync.RWMutex),
		joinServerCH:  make(chan *Client, 64),
		leaveServerCH: make(chan *Client, 64),
		broadcastCH:   make(chan *ReqMsg, 64),
	}
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{
		ReadBufferSize:  512,
		WriteBufferSize: 512,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Printf("Error on HTTP conn upgrade %v\n", err)
		return
	}

	// add conn to server
	client := NewClient(conn)
	s.joinServerCH <- client
	go client.readMsgLoop(s)
	// read messages
}

func (s *Server) AcceptLoop() {
	for {
		select {
		case c := <-s.joinServerCH:
			s.joinServer(c)
		case c := <-s.leaveServerCH:
			s.LeaveServer(c)
		case msg := <-s.broadcastCH:
			s.broadcast(msg)
		}
	}
}

func (s *Server) joinServer(c *Client) {
	s.clients[c.ID] = *c
	fmt.Printf("Client Joind the Server, CID = %s\n", c.ID)
}

func (s *Server) broadcast(msg *ReqMsg) {

}

func (s *Server) LeaveServer(c *Client) {
	delete(s.clients, c.ID)
	fmt.Printf("Client Leave the Server CID = %s\n", c.ID)
}

func createWSServer() {
	s := NewServer()
	go s.AcceptLoop()
	http.HandleFunc("/", s.handleWS)

	fmt.Printf("starting server on port: %s\n", WSPort)
	log.Fatal(http.ListenAndServe(WSPort, nil))
}

/*
*

	TODO

[x] HTTP server
[x] Upgrade it to WS once client connects
[x] Add newly connected ws to server
[x] Add WS ckient
[] Remove client and discinnect
[] Send brodcast msg -> no race conditions

	*
*/
func main() {
	createWSServer()
}
