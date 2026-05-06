# Go WebSocket Concurrency Study

This repository is a small Go project for studying concurrency with a
WebSocket server. It intentionally keeps the application simple so the
important concurrency ideas are easy to see:

- every HTTP/WebSocket connection is handled concurrently by `net/http`
- multiple goroutines can touch shared server state at the same time
- shared mutable state must be protected with synchronization
- `go test -race` can find real data races when the racing code runs inside the test process

The server listens on port `:3223`, upgrades HTTP requests to WebSocket
connections, wraps each connection in a `Client`, and stores connected clients
inside the `Server`.

## Project Layout

```text
.
├── main.go       # WebSocket server, Client, Server, and connection handling
├── main_test.go  # Concurrent client test used to exercise the server
├── Makefile      # Build and test shortcuts
├── go.mod        # Go module definition
└── go.sum        # Dependency checksums
```

The project uses:

- Go
- `net/http` from the standard library
- `sync` from the standard library
- `github.com/gorilla/websocket`

## Server Flow

The main server setup starts in `createWSServer`:

```go
func createWSServer() {
	s := NewServer()
	http.HandleFunc("/", s.handleWS)

	fmt.Printf("starting server on port: %s\n", WSPort)
	log.Fatal(http.ListenAndServe(WSPort, nil))
}
```

The important detail is this line:

```go
http.HandleFunc("/", s.handleWS)
```

It registers `s.handleWS` as the HTTP handler. When clients connect,
`net/http` accepts each connection and serves requests concurrently. That means
`handleWS` can be running in more than one goroutine at the same time.

## Client Model

Each WebSocket connection is wrapped in a `Client`:

```go
type Client struct {
	ID   string
	mu   *sync.RWMutex
	conn *websocket.Conn
}
```

`Client` currently stores:

- `ID`: a short random identifier
- `mu`: a per-client mutex for future client-specific reads/writes
- `conn`: the underlying WebSocket connection

The client mutex is not heavily used yet, but it will become useful when the
server starts reading from and writing to clients concurrently.

## Server Model

The server tracks connected clients:

```go
type Server struct {
	clients []*Client
	mu      *sync.RWMutex
}
```

`clients` is shared mutable state. Because every new WebSocket connection can be
handled by a different goroutine, this slice must be protected whenever it is
read or written.

The current code protects appending a new client like this:

```go
client := NewClient(conn)

s.mu.Lock()
s.clients = append(s.clients, client)
s.mu.Unlock()
```

This is the key concurrency fix in the project.

## Why The Mutex Is Needed

This line looks small:

```go
s.clients = append(s.clients, client)
```

But `append` is not atomic. Under the hood, Go may need to:

1. read the current slice header
2. check the current length and capacity
3. allocate a larger backing array if needed
4. copy old elements into the new backing array
5. write the new client into the array
6. update the slice header

If two goroutines do that at the same time, they can corrupt each other's view
of the slice. One goroutine may read the slice while another is writing it, or
both may write different versions of the slice header.

That is a data race.

## What The Race Detector Shows

When the append is not protected by a mutex, this test can fail with warnings
like:

```text
WARNING: DATA RACE
Read at ... by goroutine ...
  chat-server.(*Server).handleWS()
      main.go:62

Previous write at ... by goroutine ...
  chat-server.(*Server).handleWS()
      main.go:62
```

This means two different goroutines were accessing the same memory at the same
time, and at least one of them was writing.

In this project, those goroutines are created by the HTTP server. Each incoming
connection may be handled independently, so many calls to `handleWS` can overlap.

## Test Flow

`Test_Connection` starts the WebSocket server in a goroutine:

```go
go createWSServer()
```

Then it starts multiple client goroutines:

```go
for range tc.clientCount {
	go DialServer(tc.wg)
}
```

Each client connects to the server:

```go
conn, _, err := dialer.Dial(fmt.Sprintf("%s%s", host, WSPort), nil)
```

The `sync.WaitGroup` keeps the test from finishing until every client goroutine
has completed:

```go
tc.wg.Add(tc.clientCount)
...
defer wg.Done()
...
tc.wg.Wait()
```

This creates enough concurrent connection activity to exercise the shared
`Server.clients` slice.

## WaitGroup Concept

`sync.WaitGroup` is used when one goroutine needs to wait for a group of other
goroutines to finish.

The pattern is:

```go
wg.Add(n)      // tell the WaitGroup how many goroutines to wait for
go work(&wg)  // start work in goroutines
wg.Done()     // each goroutine marks itself as finished
wg.Wait()     // block until all goroutines call Done
```

In this project, the test goroutine waits for all WebSocket clients to connect,
sleep briefly, close, and return.

## Mutex Concept

`sync.RWMutex` protects shared data.

Use `Lock` and `Unlock` when writing:

```go
s.mu.Lock()
s.clients = append(s.clients, client)
s.mu.Unlock()
```

Use `RLock` and `RUnlock` when only reading:

```go
s.mu.RLock()
count := len(s.clients)
s.mu.RUnlock()
```

A regular `sync.Mutex` would also work here. `sync.RWMutex` is useful when the
server will have many readers and fewer writers.

## Go Proverb: Do Not Communicate By Sharing Memory

Go has a well-known concurrency proverb:

```text
Do not communicate by sharing memory; instead, share memory by communicating.
```

It means goroutines should usually coordinate by sending values through
channels instead of directly reading and writing the same variable from many
places.

Sharing memory directly looks like this:

```go
// Many goroutines touch the same slice.
s.mu.Lock()
s.clients = append(s.clients, client)
s.mu.Unlock()
```

This is sometimes fine, but now every goroutine that touches `s.clients` must
remember to use the same mutex. If one goroutine forgets, the program can have a
race.

Communicating through a channel looks more like this:

```go
type Server struct {
	clients    []*Client
	registerCh chan *Client
}
```

Then connection handlers send new clients to the server:

```go
s.registerCh <- client
```

And one server goroutine owns the `clients` slice:

```go
for client := range s.registerCh {
	s.clients = append(s.clients, client)
}
```

In that design, the slice is still memory, but it is owned by one goroutine.
Other goroutines do not mutate it directly. They communicate what they want by
sending messages through a channel.

So the proverb does not mean "never use mutexes." Mutexes are normal and useful
in Go. It means:

- if many goroutines need to coordinate behavior, consider channels
- if many goroutines need to protect shared data, a mutex may be simpler
- the safest design is one where ownership of data is obvious

For this project, the current mutex approach is a good first step. A future
exercise would be to rewrite the server so one goroutine owns `clients`, and all
adds, removes, and broadcasts happen through channels.

## Running The Project

Build the server:

```sh
make build-chat
```

Run the server:

```sh
make chat
```

Run normal tests:

```sh
make test-chat
```

Run tests with the race detector:

```sh
make test-chat-race
```

Or run the race detector directly:

```sh
go test -race -v -count=1 ./...
```

`-count=1` disables test result caching for that run, which is helpful while
studying race behavior.

## Important Race Detector Lesson

The race detector only reports races that actually happen during the run.

It does not scan the code and prove that everything is safe. It instruments the
running program and watches memory accesses. So for a race to be reported:

- the code must run inside the race-instrumented process
- the racing goroutines must overlap in time
- the test must exercise the shared memory access

This matters for this project. If the WebSocket server is running as a separate
binary outside `go test -race`, the test clients can connect to it, but the race
detector will not see races inside that external server process.

To detect races in server code, start the server inside the test process, as the
current test does.

## Current Test Limitation

The current test is good for learning, but it is not yet ideal production test
style.

It starts a real server on the fixed port `:3223`:

```go
go createWSServer()
```

That means:

- the test can fail if another process is already using port `3223`
- the server is not shut down cleanly by the test
- multiple tests using the same global HTTP handler can interfere with each other

A cleaner next step would be to use `httptest.NewServer` or create an
`http.Server` with a listener that the test can close.

## Good Next Exercises

Here are useful concurrency exercises to build on this code:

1. Add a `Server.ClientCount()` method that safely reads `len(s.clients)`.
2. Remove clients from `s.clients` when they disconnect.
3. Add a broadcast method that sends a message to all connected clients.
4. Protect concurrent writes to each WebSocket connection.
5. Replace the fixed-port test with an `httptest`-based test.
6. Try removing the server mutex and run `go test -race -v -count=1 ./...` to reproduce the race.

## Core Takeaway

Goroutines make concurrency easy to start, but shared memory still needs careful
ownership.

In this project, the shared memory is:

```go
clients []*Client
```

The rule is:

> Every access to shared mutable state should have a clear synchronization
> strategy.

For this server, that strategy is `Server.mu`.
