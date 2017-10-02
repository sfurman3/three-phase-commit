package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func execute(conn net.Conn, command string) {
	args := strings.Split(command, " ")
	argLengthAtLeast := func(min int) bool {
		if len(args) < min {
			Error("not enough arguments to ",
				args[0], " command: \"", command, "\"")
			return false
		}
		return true
	}

	switch args[0] {
	case "get":
		if argLengthAtLeast(2) {
			getCoordinator(conn, args[1])
		}
	case "delete":
		// TODO: maybe remove COORDINATOR check
		if COORDINATOR == ID && argLengthAtLeast(2) {
			deleteCoordinator(args[1:])
		}
	case "add":
		// TODO: maybe remove COORDINATOR check
		if COORDINATOR == ID && argLengthAtLeast(3) {
			addCoordinator(args[1:])
		}

	case "crash":
		crash()
	case "crashAfterVote":
		crashAfterVote()
	case "crashBeforeVote":
		crashBeforeVote()
	case "crashAfterAck":
		crashAfterAck()

	default:
		if len(args) == 1 {
			switch args[0] {
			case "crashVoteREQ":
			case "crashPartialPreCommit":
			case "crashPartialCommit":
				crash()
			default:
				Error("unrecognized command: \"", command, "\"")
			}
		} else {
			switch args[0] {
			case "crashVoteREQ":
				crashVoteREQ(args[1:])
			case "crashPartialPreCommit":
				crashPretialPreCommit(args[1:])
			case "crashPartialCommit":
				crashPartialCommit(args[1:])
			default:
				Error("unrecognized command: \"", command, "\"")
			}
		}
	}
}

///////////////////////////////////////////////////////////////////////////////
// recovery   								     //
///////////////////////////////////////////////////////////////////////////////

// TODO:
// somehow you believe you are the coordinator
// - You are process 0 and you have no log
// - You are not process 0, but you detect that p0 has died (i.e. no
//   heartbeat)

// before you notify the master, you check with the other processes
// have no records; if they do, then they did something without your knowledge

func recoverFromFailure() {
	// TODO: there's no log file OR the log file is empty; you were either
	// a coordinator that never initiated a 3PC protocol OR you were a
	// participant that either never received a VOTE-REQ or crashed before
	// you could write your vote to the log; You have to wait until you get
	// a message from someone else

	// if your id is 0, you are the current coordinator

	// if your id is 0 and you have no log (OR your log is empty)

	// TODO: open the DT Log and try to read the latest value
	var penultEntry string
	var lastEntry string

	// recovered coordinator
	if lastEntry == "start-3PC" {
		// TODO: failed before sending a decision
		// TODO: write abort to the DT log
		// TODO: send abort to participants
	}
	if penultEntry == "start-3PC" {
	}

	switch lastEntry {
	case "start-3PC":
		// TODO: I was the coordinator
	case "commit":
		switch penultEntry {
		case "start-3PC":
			// TODO: I decided commit
		case "abort":
		case "commit":
		}
	case "abort":
		if penultEntry == "start-3PC" {
			// TODO: I decided commit
		}
	default:
		// TODO
	}
}

///////////////////////////////////////////////////////////////////////////////
// coordinator								     //
///////////////////////////////////////////////////////////////////////////////

// TODO
type response struct {
	v  string
	c  net.Conn
	id int
}

// TODO
func getCoordinator(conn net.Conn, song string) {
	// check the local playlist
	url := LocalPlaylist.GetSongUrl(song)

	// ask other servers for the song url (if this server doesn't have it)
	if url == "NONE" {
		m := newMessage("get " + song)
		mBytes, err := json.Marshal(m)
		if err != nil {
			Error("failed to create message: \"", "get ", song, "\"")
			return
		}
		mJson := string(mBytes)

		for id := 0; id < NUM_PROCS; id++ {
			if id == ID {
				continue
			}

			resp, err := sendAndWaitForResponse(mJson, id)
			if err != nil {
				continue
			}

			args := strings.Split(string(resp), " ")
			if len(args) >= 2 {
				if args[0] == "resp" && args[1] != "NONE" {
					url = string(args[1])
					LocalPlaylist.AddOrUpdateSong(song, url)
					// TODO: Write the new value to the DT
					// log?
					//
					// MAYBE NOT because you might get a
					// value that is about to be removed in
					// a current commit and then you'll
					// create an inconsistent state
					break
				}
			}
		}
	}

	fmt.Fprintln(conn, "resp", url)
}

// TODO
func addCoordinator(args []string) {
	song := args[0]
	url := args[1]
	coordinatorVote := vote(url)

	// TODO: maybe write start-3pc first
	// abort immediately if the coordinator votes no
	if coordinatorVote == "no" {
		MessagesToMaster.Enqueue("ack abort")
		return
	}

	// write start-3pc record in DT log
	writeToDtLog("start-3pc add", song, url)

	// send VOTE-REQ to all participants
	// AND wait for vote messages from all participants
	resps, err, timeout := broadcastToParticipantsAndAwaitResponses(
		fmt.Sprintf("vote-req add %s %s", song, url))
	if timeout {
		// write abort record in DT log
		writeToDtLog("abort")

		// send abort to all processes that voted yes
		sendAbortToYesVoters(resps)

		// send abort to master
		MessagesToMaster.Enqueue("ack abort")
		return
	} else if err != nil {
		Error(err)
		return
	}

	// check that all participants voted yes
	allVotedYes := true
	for _, resp := range resps {
		if resp.v != "yes" {
			allVotedYes = false
			break
		}
	}

	if allVotedYes {
		// send pre-commit to all participants
		sendToParticipantsAndAwaitAcks(resps, "pre-commit")

		// write commit record to DT log
		writeToDtLog("commit")

		// send commit to all participants
		sendToParticipantsAndAwaitAcks(resps, "commit")

		// send commit to master
		MessagesToMaster.Enqueue("ack commit")

		// add song to local playlist
		LocalPlaylist.AddOrUpdateSong(song, url)
	} else {
		// some participant voted no

		// write abort record in DT log
		writeToDtLog("abort")

		// send abort to all processes that voted yes
		sendAbortToYesVoters(resps)

		// send abort to master
		MessagesToMaster.Enqueue("ack abort")
	}

	return
}

// TODO
func deleteCoordinator(args []string) {
	song := args[0]

	// write start-3pc record in DT log
	writeToDtLog("start-3pc delete", song)

	// send VOTE-REQ to all participants
	// AND wait for vote messages from all participants
	resps, err, timeout := broadcastToParticipantsAndAwaitResponses(
		"vote-req delete " + song)
	if timeout {
		// write abort record in DT log
		writeToDtLog("abort")

		// send abort to all processes that voted yes
		sendAbortToYesVoters(resps)

		// send abort to master
		MessagesToMaster.Enqueue("ack abort")
		return
	} else if err != nil {
		Error(err)
		return
	}

	// check that all participants voted yes
	allVotedYes := true
	for _, resp := range resps {
		if resp.v != "yes" {
			allVotedYes = false
			break
		}
	}

	if allVotedYes {
		// send pre-commit to all participants
		sendToParticipantsAndAwaitAcks(resps, "pre-commit")

		// write commit record to DT log
		writeToDtLog("commit")

		// send commit to all participants
		sendToParticipantsAndAwaitAcks(resps, "commit")

		// send commit to master
		MessagesToMaster.Enqueue("ack commit")

		// add song to local playlist
		LocalPlaylist.DeleteSong(song)
	} else {
		// some participant voted no

		// write abort record in DT log
		writeToDtLog("abort")

		// send abort to all processes that voted yes
		sendAbortToYesVoters(resps)

		// send abort to master
		MessagesToMaster.Enqueue("ack abort")
	}

	return
}

func updateCoordinator(update string, args []string) {
	song := args[0]
	url := args[1]
	coordinatorVote := vote(url)
	operation := update + " " + song + " " + url

	// abort immediately if the coordinator votes no
	if coordinatorVote == "no" {
		MessagesToMaster.Enqueue("ack abort")
		return
	}

	// write start-3pc record in DT log
	writeToDtLog("start-3pc", operation)

	// send VOTE-REQ to all participants
	// AND wait for vote messages from all participants
	resps, err, timeout := broadcastToParticipantsAndAwaitResponses(
		fmt.Sprintf("vote-req %s", operation))
	if timeout {
		// write abort record in DT log
		writeToDtLog("abort")

		// send abort to all processes that voted yes
		sendAbortToYesVoters(resps)

		// send abort to master
		MessagesToMaster.Enqueue("ack abort")
		return
	} else if err != nil {
		Error(err)
		return
	}

	// check that all participants voted yes
	allVotedYes := true
	for _, resp := range resps {
		if resp.v != "yes" {
			allVotedYes = false
			break
		}
	}

	if allVotedYes {
		// send pre-commit to all participants
		sendToParticipantsAndAwaitAcks(resps, "pre-commit")

		// write commit record to DT log
		writeToDtLog("commit")

		// send commit to all participants
		sendToParticipantsAndAwaitAcks(resps, "commit")

		// send abort to master
		MessagesToMaster.Enqueue("ack commit")

		// add song to local playlist
		LocalPlaylist.AddOrUpdateSong(song, url)
	} else {
		// some participant voted no

		// write abort record in DT log
		writeToDtLog("abort")

		// send abort to all processes that voted yes
		sendAbortToYesVoters(resps)

		// send abort to master
		MessagesToMaster.Enqueue("ack abort")
	}

	return
}

func broadcastToParticipantsAndAwaitResponses(msg string) ([]response, error, bool) {
	type connection struct {
		c  net.Conn
		id int
	}

	var responses []response
	var conns []connection
	timeout := false

	msgBytes, err := json.Marshal(newMessage(msg))
	if err != nil {
		return nil, err, timeout
	}
	msgJSON := string(msgBytes)

	// send message to participants
	for id := 0; id < NUM_PROCS; id++ {
		if id == ID {
			continue
		}

		var conn net.Conn
		conn, err = net.Dial("tcp", ":"+strconv.Itoa(START_PORT+id))
		if err == nil {
			_, err = fmt.Fprintln(conn, msgJSON)
			if err == nil {
				conns = append(conns, connection{conn, id})
			} else {
				conn.Close()
			}
		}
	}

	// wait for responses from all recipients
	for _, conn := range conns {
		conn.c.SetDeadline(time.Now().Add(TIMEOUT))
		r := bufio.NewReader(conn.c)

		var resp string
		resp, err = r.ReadString('\n')
		resp = strings.TrimSpace(resp)
		if err == nil {
			responses = append(responses, response{resp, conn.c, conn.id})
		} else {
			timeout = true
		}
	}

	return responses, err, timeout
}

func sendAbortToYesVoters(resps []response) {
	abortBytes, err := json.Marshal(newMessage("abort"))
	if err != nil {
		Error("failed to marshal \"abort\" into message")
		return
	}
	abortJson := string(abortBytes)
	for _, resp := range resps {
		if resp.v == "yes" {
			// send abort
			conn, err := net.Dial("tcp", ":"+strconv.Itoa(START_PORT+resp.id))
			defer conn.Close()
			if err == nil {
				fmt.Fprintln(conn, abortJson)
			}
		}
	}
}

func sendToParticipantsAndAwaitAcks(participants []response, msg string) {
	// send message to participants
	for i, ptc := range participants {
		if ptc.id == ID {
			continue
		}

		defer ptc.c.Close()
		_, err := fmt.Fprintln(ptc.c, msg)
		if err != nil {
			participants[i].c = nil
		}
	}

	// wait for responses from all recipients
	for _, ptc := range participants {
		if ptc.c != nil {
			ptc.c.SetDeadline(time.Now().Add(TIMEOUT))
			r := bufio.NewReader(ptc.c)

			// read ack from recipient
			r.ReadString('\n')
		}
	}
}

///////////////////////////////////////////////////////////////////////////////
// participant								     //
///////////////////////////////////////////////////////////////////////////////

func getParticipant(conn net.Conn, song string) {
	url := LocalPlaylist.GetSongUrl(song)
	fmt.Fprintln(conn, "resp", url)
}

func addParticipant(conn net.Conn, song, url string) {
	vote := vote(url)
	if vote == "yes" {
		// write yes record in DT log
		writeToDtLog("yes add", song, url)

		// vote yes
		fmt.Fprintln(conn, "yes")

		// wait for message from coordinator
		msg, err, timeout := waitForMessageFromCoordinator(conn)
		if timeout {
			initiateElectionProtocol()
			return
		} else if err != nil {
			Error(err)
			return
		}

		if msg == "pre-commit" {
			// send ack to coordinator
			fmt.Fprintln(conn, "ack")

			// wait for commit from coordinator
			msg, err, timeout := waitForMessageFromCoordinator(conn)
			if timeout {
				initiateElectionProtocol()
				return
			} else if err != nil {
				Error(err)
				return
			}

			if msg == "commit" {
				// write commit record in DT log
				writeToDtLog("commit")

				// add song to local playlist
				LocalPlaylist.AddOrUpdateSong(song, url)
			} else {
				Error("coordinator did not respond commit: ", msg)
			}
		} else if msg == "abort" {
			// write abort record in DT log
			writeToDtLog("abort")
		} else {
			Error("unrecognized response from coordinator: ", msg)
			return
		}
	} else {
		// vote yes
		fmt.Fprintln(conn, "no")

		// write abort record in DT log
		writeToDtLog("abort")
	}
}

func deleteParticipant(conn net.Conn, song string) {
	// write yes record in DT log
	writeToDtLog("yes delete", song)

	// vote yes
	fmt.Fprintln(conn, "yes")

	// wait for message from coordinator
	msg, err, timeout := waitForMessageFromCoordinator(conn)
	if timeout {
		initiateElectionProtocol()
		return
	} else if err != nil {
		Error(err)
		return
	}

	if msg == "pre-commit" {
		// send ack to coordinator
		fmt.Fprintln(conn, "ack")

		// wait for commit from coordinator
		msg, _, timeout := waitForMessageFromCoordinator(conn)
		if timeout {
			initiateElectionProtocol()
			return
		}

		if msg == "commit" {
			// write commit record in DT log
			writeToDtLog("commit")

			// delete song from local playlist
			LocalPlaylist.DeleteSong(song)
		} else {
			Error("coordinator did not respond commit: ", msg)
		}
	} else if msg == "abort" {
		// write abort record in DT log
		writeToDtLog("abort")
	} else {
		Error("unrecognized response from coordinator: ", msg)
		return
	}
}

func vote(url string) string {
	if len(url) > ID+5 {
		return "no"
	} else {
		return "yes"
	}
}

func waitForMessageFromCoordinator(conn net.Conn) (string, error, bool) {
	r := bufio.NewReader(conn)
	// increase the TIMEOUT because a msg must be sent to each other
	// participant
	conn.SetDeadline(time.Now().Add(TIMEOUT * time.Duration(NUM_PROCS)))
	response, err := r.ReadString('\n')
	if err != nil {
		netErr, ok := err.(net.Error)
		if ok && netErr.Timeout() {
			Error("timed out when reading response from coordinator")
			return "", err, true
		} else {
			Error(err)
			return "", err, false
		}
	}

	return response, nil, false
}

// TODO
func initiateElectionProtocol() {
	// TODO: initiate election protocol

	// TODO: if elected, invoke coordinator's algorithm of
	// termination protocol

	// TODO: else, invoke participant's algorithm of
	// termination protocol
}

///////////////////////////////////////////////////////////////////////////////
// crash      								     //
///////////////////////////////////////////////////////////////////////////////

func crash() {
	Fatal("CRASH")
}

func crashAfterVote()                     {}
func crashBeforeVote()                    {}
func crashAfterAck()                      {}
func crashVoteREQ(args []string)          {}
func crashPretialPreCommit(args []string) {}
func crashPartialCommit(args []string)    {}

///////////////////////////////////////////////////////////////////////////////
// DT log     								     //
///////////////////////////////////////////////////////////////////////////////

func writeToDtLog(entry ...interface{}) {
	file, err := os.OpenFile(DT_LOG, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	defer file.Close()
	if err != nil {
		Error(err)
		return
	}

	fmt.Fprintln(file, entry...)
}
