// Server is an implementation of a distributed, FIFO consistent chatroom where
// participants (servers) can broadcast messages and detect failures. Each
// server keeps a FIFO log of messages it has received.
//
// "server [id] [numservers] [port]" sets up a server with ID [id] on port
// [20000 + id] with a master-facing port of [port] (i.e the port which
// the master process uses to issue commands and accept responses).
// [numservers] is the total number of servers in the system, and is used to
// connect to the remaining servers. A system of n servers is assumed to have
// server IDs {0...n-1} and ports {20000...20000 + n-1} respectively.
//
//  The following master commands are supported:
//  --------------------------------------------
//  - "get\n:               return a list of all received messages
//  - "alive\n":            return a list of server IDs believed to be alive
//  - "broadcast <m>\n":    send <m> to everyone alive (including the sender)
//
//  Responses have the following format:
//  ------------------------------------
//  - "get\n"   -> "messages <msg1>,<msg2>,...\n"
//  - "alive\n" -> "alive <id1>,<id2>,...\n"
//
// You can test a server instance using netcat. For example:
//  ➜  server 0 1 30000 &
//  [2] 43246
//  ➜  netcat localhost 30000
//  get                         (command)
//  messages
//  alive                       (command)
//  alive 0
//  broadcast hello world       (command)
//  get                         (command)
//  messages hello world
//  ^C
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// Base port for servers in the system
	// Port numbers are always START_PORT + ID
	START_PORT = 20000

	// Duration between heartbeat messages (i.e. empty messages broadcasted
	// to other servers to indicate the server is alive)
	HEARTBEAT_INTERVAL = 200 * time.Millisecond

	// Maximum interval after the send timestamp of the last message
	// received from a server for which the sender is considered alive
	ALIVE_INTERVAL = 200 * time.Millisecond

	// Timeout for waiting for a response from the coordinator
	TIMEOUT = 10 * time.Millisecond

	// Constants for printing error messages to the terminal
	BOLD_RED = "\033[31;1m"
	NO_STYLE = "\033[0m"
	ERROR    = "[" + BOLD_RED + "ERROR" + NO_STYLE + "]"
)

var (
	ID                 = -1 // id of the server {0, ..., NUM_PROCS-1}
	NUM_PROCS          = -1 // total number of servers
	MASTER_PORT        = -1 // number of the master-facing port
	REQUIRED_ARGUMENTS = []*int{&ID, &NUM_PROCS, &MASTER_PORT}

	PORT        = -1   // server's port number
	COORDINATOR = -1   // coordinator's id number
	DT_LOG      string // name of server's DT Log file

	LocalPlaylist    playlist         // in-memory copy of server's playlist
	MessagesFIFO     tsMsgQueue       // all received messages in FIFO order
	LastTimestamp    tsTimestampQueue // timestamp of last message from each server
	MessagesToMaster tsStringQueue    // pending messages to master
)

// init parses and validates command line arguments (by name or position) and
// initializes global variables
func init() {
	setArgsPositional()

	if NUM_PROCS <= 0 {
		Fatal("invalid number of servers: ", NUM_PROCS)
	}

	logDir := "logs"

	PORT = START_PORT + ID
	DT_LOG = fmt.Sprintf("%s/dt_log_%0*d.log", logDir, len(os.Args[2]), ID)

	LocalPlaylist = NewPlaylist()

	LastTimestamp.value = make([]time.Time, NUM_PROCS)

	// make directories for storing logs and playlists
	fileMode := os.ModePerm | os.ModeDir
	os.Mkdir(logDir, fileMode)
}

///////////////////////////////////////////////////////////////////////////////
// server                                                                    //
///////////////////////////////////////////////////////////////////////////////

func main() {
	// Bind the server-facing port and listen for messages
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(PORT))
	if err != nil {
		Fatal("failed to bind server-facing port: ", strconv.Itoa(PORT))
	}
	go fetchMessages(ln)
	go heartbeat()
	serveMaster()
}

func determineInitialCoordinator(ln net.Listener) {
	// broadcast some empty messages to indicate the server is alive
	broadcast(emptyMessage())

	time.Sleep(1000 * time.Millisecond) // wait for other servers to spin up

	// determine the coordinator's identity
	COORDINATOR = LastTimestamp.LowestIdAlive()
	// TODO: remove
	fmt.Println(ID, "elected", COORDINATOR)
	if COORDINATOR == ID {
		// tell the master that this server is the coordinator
		MessagesToMaster.Enqueue("coordinator " + strconv.Itoa(ID))
	}
}

// heartbeat sleeps for HEARTBEAT_INTERVAL and broadcasts an empty message to
// every server to indicate that the server is still alive
func heartbeat() {
	for {
		go broadcast(emptyMessage())
		time.Sleep(HEARTBEAT_INTERVAL)
	}
}

// fetchMessages retrieves messages from other servers and adds them to the
// log, listening on PORT (i.e. START_PORT + PORT)
func fetchMessages(ln net.Listener) {
	type acceptResult struct {
		conn net.Conn
		err  error
	}

	var lock sync.Mutex
	connChan := make(chan acceptResult)
	electChan := make(chan bool)
	go func() {
		for {
			conn, err := ln.Accept()
			connChan <- acceptResult{conn, err}
		}
	}()
	go func() {
		for {
			lock.Lock()
			if COORDINATOR != ID && !LastTimestamp.IsAlive(COORDINATOR) {
				elected, _ := initiateElectionProtocol()
				electChan <- elected
			}
			lock.Unlock()
		}
	}()

	for {
		select {
		case ar := <-connChan:
			if ar.err == nil {
				lock.Lock()
				handleMessage(ar.conn)
				lock.Unlock()
			}
		case <-electChan:
		}
	}
}

// handleMessage retrieves the first message from conn, adds it to the log, and
// closes the connection. It also updates LastTimestamp for the sending server.
//
// NOTE: This function must be called sequentially (NOT by starting a new
// thread for each new connection) in order to maintain FIFO receipt.
// Otherwise, depending on scheduling, a message B may be added to MessagesFIFO
// before another message A, even though A connected first.
//
// The disadvantage is that, if the delivery of a message is blocked (e.g. the
// sender died before it could terminate the message with a '\n'), then all of
// the subsequent messages to be delivered are also blocked, possibly FOREVER.
//
// NOTE: If FIFO receipt is no longer necessary, we can simply sort
// MessagesFIFO by send timestamp in order to approximate the send order. We
// could also use a causal delivery method provided by a data structure such as
// the vector.MessageReceptacle to deliver messages based on causal precedence.
func handleMessage(conn net.Conn) {
	defer conn.Close()

	messenger := bufio.NewReader(conn)
	msgBytes, err := messenger.ReadBytes('\n')
	if err != nil {
		return
	}

	msg := new(Message)
	err = json.Unmarshal(msgBytes, msg)
	if err != nil {
		return
	}

	// update LastTimestamp for the sender
	// NOTE: assumes message IDs are in {0..n-1}
	LastTimestamp.UpdateTimestamp(msg)

	// update the COORDINATOR if higher (i.e. the coordinator has died)
	if msg.coordinator > COORDINATOR {
		COORDINATOR = msg.coordinator

		if COORDINATOR == ID {
			// tell the master that this server is the coordinator
			MessagesToMaster.Enqueue("coordinator " + strconv.Itoa(ID))
		}
	}

	if len(msg.Content) == 0 { // msg is an empty message
		return
	}

	// TODO: REMOVE
	fmt.Println(ID, "received", msg.Content)

	args := strings.Split(msg.Content, " ")
	argLengthAtLeast := func(min int) bool {
		if len(args) < min {
			Error("not enough arguments to ",
				args[0], " command: \"", msg.Content, "\"")
			return false
		}
		return true
	}

	switch args[0] {
	case "get":
		if argLengthAtLeast(2) {
			getParticipant(conn, args[1])
		}
	case "vote-req":
		if argLengthAtLeast(3) {
			if args[1] == "delete" {
				deleteParticipant(conn, args[2])
			} else if argLengthAtLeast(4) {
				if args[1] == "add" {
					addParticipant(conn, args[2], args[3])
				} else {
					Error("no such vote-req operation: \"",
						strings.Join(args, " "), "\"")
				}
			} else {
				Error("no such vote-req operation: \"",
					strings.Join(args, " "), "\"")
			}
		}
	default:
		// TODO
	}

	// TODO: REMOVE
	MessagesFIFO.Enqueue(msg)
}

// serveMaster listens on MASTER_PORT for a connection from a master process
// and services its commands
//
// NOTE: only one master process is served at any given time
func serveMaster() {
	// Bind the master-facing port and start listening for commands
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(MASTER_PORT))
	if err != nil {
		Fatal("failed to bind master-facing port: ",
			strconv.Itoa(MASTER_PORT))
	}

	for {
		masterConn, err := ln.Accept()
		if err != nil {
			continue
		}

		handleMaster(masterConn)
	}
}

// handleMaster executes commands from the master process and responds with any
// requested data
func handleMaster(masterConn net.Conn) {
	defer masterConn.Close()
	master := bufio.NewReader(bufio.NewReader(masterConn))

	for {
		// TODO: replace with direct writes to master connection in child
		// calls
		//
		// send the next pending message to master
		msg := MessagesToMaster.Dequeue()
		if msg != "" {
			if _, err := fmt.Fprintln(masterConn, msg); err != nil {
				MessagesToMaster.PushFront(msg)
			}
		}

		// check for a new command from master
		masterConn.SetReadDeadline(time.Now().Add(TIMEOUT))
		command, err := master.ReadString('\n')
		if err != nil {
			netErr, ok := err.(net.Error)
			if ok && netErr.Timeout() {
				continue
			}
			// connection to master lost
			return
		}

		command = strings.TrimSpace(command)
		// TODO: REMOVE
		fmt.Println(ID, "received:", command)
		execute(masterConn, command)
	}
}

// TODO: Delete?
func writeMessages(rwr *bufio.ReadWriter) {
	rwr.WriteString("messages ")
	MessagesFIFO.WriteMessages(rwr)
	rwr.WriteByte('\n')

	err := rwr.Flush()
	if err != nil {
		Fatal(err)
	}
}

// TODO: Delete?
func writeAlive(rwr *bufio.ReadWriter) {
	now := time.Now()

	rwr.WriteString("alive ")
	LastTimestamp.WriteAlive(rwr, now)
	rwr.WriteByte('\n')

	err := rwr.Flush()
	if err != nil {
		Fatal(err)
	}
}

// broadcast sends the given message to all other servers (including itself and
// excluding the master)
//
// NOTE: Sends are sequential, so that broadcast does not return until an
// attempt has been made to send the message to all servers
//
// NOTE: This function must be called sequentially (NOT by starting a new
// thread for each new message) in order to maintain FIFO receipt. Otherwise,
// depending on scheduling, a message B could be broadcast to a server before
// another message A, even though A's thread was started first.
//
// The disadvantage is that, if the receipt of one message is delayed for any
// of its recipients, then all of the subsequent commands sent by the master
// are also delayed (until the send times out). This may cause servers to not
// receive the message on time. This is likely not an issue when working with a
// small number of servers.
//
// NOTE: If FIFO receipt is no longer necessary, the recipient can simply sort
// delivered messages by send timestamp in order to approximate the send order.
// They could also use a causal delivery method provided by a data structure
// such as the vector.MessageReceptacle to deliver messages based on causal
// precedence.
func broadcast(msg *Message) {
	// Convert to JSON
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return
	}
	msgJSON := string(msgBytes)

	// send non-empty messages to self
	if len(msg.Content) != 0 {
		MessagesFIFO.Enqueue(msg)
	}

	// send message to other servers
	for id := 0; id < NUM_PROCS; id++ {
		if id == ID {
			continue
		}

		sendMarshaled(msgJSON, id)
	}
}

// send a message to the server with the given id
func sendMarshaled(msg string, id int) error {
	// TODO: In the future, you may want to consider using
	// net.DialTimeout (e.g. the recipient is so busy it cannot
	// service the send in a reasonable amount of time) and/or
	// consider starting a new thread for every send to prevent
	// sends from blocking each other (the timeout might help
	// prevent a buildup of threads that can't progress)
	conn, err := net.Dial("tcp", ":"+strconv.Itoa(START_PORT+id))
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = fmt.Fprintln(conn, msg)
	return err
}

// sendAndWaitForResponse takes a Message marshaled into JSON and tries to send
// it to the server with the given id. Returns the response with any leading or
// trailing whitespace removed.
//
// Returns an error whose value is "timeout" if the recipient fails to respond
// within a period of TIMEOUT.
//
// Returns an error whose value is "empty response" if the recipient sends an
// empty response.
func sendAndWaitForResponse(msg string, id int) ([]byte, error) {
	conn, err := net.Dial("tcp", ":"+strconv.Itoa(START_PORT+id))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(TIMEOUT))

	_, err = fmt.Fprintln(conn, msg)
	if err != nil {
		return nil, err
	}

	r := bufio.NewReader(conn)
	resp, err := r.ReadBytes('\n')
	if err != nil {
		if netErr := err.(net.Error); netErr.Timeout() {
			// TODO: Update UP set?
			return nil, errors.New("timeout")
		}
		return nil, err
	}

	if resp == nil {
		return nil, errors.New("empty response")
	}

	return bytes.TrimSpace(resp), nil
}
