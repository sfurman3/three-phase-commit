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
	END_PORT   = 30000 - 1

	// Duration between heartbeat messages (i.e. empty messages broadcasted
	// to other servers to indicate the server is alive)
	HEARTBEAT_INTERVAL = 200 * time.Millisecond

	// Maximum interval after the send timestamp of the last message
	// received from a server for which the sender is considered alive
	ALIVE_INTERVAL = 250 * time.Millisecond

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
	HEART_PORT  = -1   // port used for receiving heartbeats
	COORDINATOR = -1   // coordinator's id number
	DT_LOG      string // name of server's DT Log file

	LocalPlaylist    playlist         // in-memory copy of server's playlist
	LastTimestamp    tsTimestampQueue // timestamp of last message from each server
	MessagesToMaster tsStringQueue    // pending messages to master

	// participant crash booleans
	CrashAfterVote  bool // participant crashes after sending vote
	CrashBeforeVote bool // participant crashes after receiving vote-req but before sending vote
	CrashAfterAck   bool // participant crashes after sending ack

	// coordinator crash booleans
	CrashVoteREQ          bool // crash after sending vote requests to participants
	CrashPartialPreCommit bool // crash after sending precommit to participants
	CrashPartialCommit    bool // crash after sending commit to participants

	CrashVoteREQList          []int
	CrashPartialPreCommitList []int
	CrashPartialCommitList    []int
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
	HEART_PORT = END_PORT - ID
	DT_LOG = fmt.Sprintf("%s/dt_log_%0*d.log", logDir, len(os.Args[2]), ID)

	LocalPlaylist = NewPlaylist()

	LastTimestamp.value = make([]time.Time, NUM_PROCS)
	for i := range LastTimestamp.value {
		LastTimestamp.value[i] = time.Now()
	}

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

// heartbeat sleeps for HEARTBEAT_INTERVAL and broadcasts an empty message to
// every server to indicate that the server is still alive
func heartbeat() {
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(END_PORT-ID))
	if err != nil {
		Fatal(err)
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				continue
			}

			rdr := bufio.NewReader(conn)
			msg, err := rdr.ReadString('\n')
			if err != nil {
				continue
			}

			msg = strings.TrimSpace(msg)
			args := strings.Split(msg, " ")
			id, err := strconv.Atoi(args[0])
			if err != nil {
				continue
			}
			coordinator, err := strconv.Atoi(args[1])
			if err != nil {
				continue
			}

			// update the coordinator if another process has a
			// higher one (i.e. it was elected before the current
			// process). this avoids the issue of multiple
			// coordinators. for example imagine 2 spins up and
			// elects itself, then 0 spins up and elects itself) in
			// this case neither thinks the coordinator has died,
			// so neither updates the coordinator
			if coordinator > COORDINATOR {
				COORDINATOR = coordinator
				MessagesToMaster.Enqueue("coordinator " + strconv.Itoa(COORDINATOR))
			}
			LastTimestamp.UpdateTimestampById(id)
		}
	}()

	for {
		go func() {
			for id := 0; id < ID; id++ {
				conn, err := net.Dial("tcp", ":"+strconv.Itoa(END_PORT-id))
				if err == nil {
					defer conn.Close()
					fmt.Fprintln(conn, ID, COORDINATOR)
				}
			}
			for id := ID + 1; id < NUM_PROCS; id++ {
				conn, err := net.Dial("tcp", ":"+strconv.Itoa(END_PORT-id))
				if err == nil {
					defer conn.Close()
					fmt.Fprintln(conn, ID, COORDINATOR)
				}
			}
		}()
		time.Sleep(HEARTBEAT_INTERVAL)
	}
}

// fetchMessages retrieves messages from other servers and adds them to the
// log, listening on PORT (i.e. START_PORT + PORT)
func fetchMessages(ln net.Listener) {
	var lock sync.Mutex
	go func() {
		for {
			lock.Lock()
			if COORDINATOR != ID && !LastTimestamp.IsAlive(COORDINATOR) {
				alive := LastTimestamp.GetAlive(time.Now())
				COORDINATOR = alive[0]
				if COORDINATOR == ID {
					MessagesToMaster.Enqueue("coordinator " + strconv.Itoa(ID))
				}
			}
			lock.Unlock()
		}
	}()

	for {
		conn, err := ln.Accept()

		if err == nil {
			lock.Lock()
			handleMessage(ln, conn)
			lock.Unlock()
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
func handleMessage(ln net.Listener, conn net.Conn) {
	defer conn.Close()

	messenger := bufio.NewReader(conn)
	msgBytes, err := messenger.ReadBytes('\n')
	if err != nil {
		return
	}

	msg := new(Message)
	err = json.Unmarshal(msgBytes, msg)
	if err != nil {
		req := string(msgBytes)
		args := strings.Split(req, " ")
		if args[0] != "state-req" {
			return
		}

		handleStateReq(ln, conn, req, args)
		return
	}

	if len(msg.Content) == 0 { // msg is an empty message
		return
	}

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
				deleteParticipant(ln, conn, args[2])
			} else if argLengthAtLeast(4) {
				if args[1] == "add" {
					addParticipant(ln, conn, args[2], args[3])
				} else {
					Error("no such vote-req operation: \"",
						strings.Join(args, " "), "\"")
				}
			} else {
				Error("no such vote-req operation: \"",
					strings.Join(args, " "), "\"")
			}
		}
	}
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
		execute(masterConn, command)
	}
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
			return nil, errors.New("timeout")
		}
		return nil, err
	}

	if resp == nil {
		return nil, errors.New("empty response")
	}

	return bytes.TrimSpace(resp), nil
}
