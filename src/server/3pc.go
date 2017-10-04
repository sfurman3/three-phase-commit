package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
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
		writeToDtLog("abort add", song, url)

		// send abort to all processes that voted yes
		sendAbortToYesVoters(resps)

		// send abort to master
		MessagesToMaster.Enqueue("ack abort")
		return
	} else if err != nil {
		// TODO: COMBINE THIS WITH THE PREVIOUS CHECK AND DO LIKEWISE
		// FOR THE REST OF THIS FILE
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
		writeToDtLog("commit add", song, url)

		// send commit to all participants
		sendToParticipants(resps, "commit")

		// send commit to master
		MessagesToMaster.Enqueue("ack commit")

		// add song to local playlist
		LocalPlaylist.AddOrUpdateSong(song, url)
	} else {
		// some participant voted no

		// write abort record in DT log
		writeToDtLog("abort add", song, url)

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
		writeToDtLog("abort delete", song)

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
		writeToDtLog("commit delete", song)

		// send commit to all participants
		sendToParticipants(resps, "commit")

		// send commit to master
		MessagesToMaster.Enqueue("ack commit")

		// add song to local playlist
		LocalPlaylist.DeleteSong(song)
	} else {
		// some participant voted no

		// write abort record in DT log
		writeToDtLog("abort delete", song)

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

	// send message to operational participants
	for _, id := range LastTimestamp.GetAlive(time.Now()) {
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

func broadcastToParticipantsAndAwaitResponsesTermination(participants []int, msg string) []response {
	type connection struct {
		c  net.Conn
		id int
	}

	var responses []response
	var conns []connection

	msgBytes, err := json.Marshal(newMessage(msg))
	if err != nil {
		return nil
	}
	msgJSON := string(msgBytes)

	// send message to participants
	for _, id := range participants {
		// TODO: delete?
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
		}
	}

	return responses
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
	sendToParticipants(participants, msg)

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

func sendToParticipants(participants []response, msg string) {
	// send message to participants
	for i, ptc := range participants {
		_, err := fmt.Fprintln(ptc.c, msg)
		if err != nil {
			participants[i].c = nil
		}
	}
}

func sendToUncertainParticipantsAndAwaitAcks(participants []response, msg string) {
	// send message to participants
	sendToUncertainParticipants(participants, msg)

	// wait for responses from all recipients
	for _, ptc := range participants {
		if ptc.c != nil && ptc.v == "uncertain" {
			ptc.c.SetDeadline(time.Now().Add(TIMEOUT))
			r := bufio.NewReader(ptc.c)

			// read ack from recipient
			r.ReadString('\n')
		}
	}
}

func sendToUncertainParticipants(participants []response, msg string) {
	// send message to participants
	for i, ptc := range participants {
		if ptc.v == "uncertain" {
			_, err := fmt.Fprintln(ptc.c, msg)
			if err != nil {
				participants[i].c = nil
			}
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

func addParticipant(ln net.Listener, conn net.Conn, song, url string) {
	vote := vote(url)
	if vote == "yes" {
		// write yes record in DT log
		writeToDtLog("yes add", song, url)

		// vote yes
		fmt.Fprintln(conn, "yes")

		// wait for message from coordinator
		msg, err, timeout := waitForMessageFromCoordinator(conn)
		if timeout {
			elected, participants := initiateElectionProtocol()
			if elected {
				// invoke coordinator's algorithm of
				// termination protocol
				addTerminationProtocolCoordinator(participants, song, url)
			} else {
				// invoke participant's algorithm of
				// termination protocol

				// wait for connection from coordinator
				lnr := (ln).(*net.TCPListener)
			startYes:
				lnr.SetDeadline(time.Now().Add(TIMEOUT * time.Duration(NUM_PROCS)))
				conn, err := lnr.Accept()
				if netErr := err.(net.Error); netErr.Timeout() {
					elected, participants := initiateElectionProtocol()
					if elected {
						addTerminationProtocolCoordinator(participants, song, url)
					} else {
						goto startYes
					}
				}

				addTerminationProtocolParticipant(conn, song, url)
			}
			return
		} else if err != nil {
			Error(err)
			return
		}

		if msg == "pre-commit" {
			// write pre-commit record in DT log
			writeToDtLog("pre-commit add", song, url)

			// send ack to coordinator
			fmt.Fprintln(conn, "ack")

			// wait for commit from coordinator
			msg, err, timeout := waitForMessageFromCoordinator(conn)
			if timeout {
				elected, participants := initiateElectionProtocol()
				if elected {
					// invoke coordinator's algorithm of
					// termination protocol
					addTerminationProtocolCoordinator(participants, song, url)
				} else {
					// invoke participant's algorithm of
					// termination protocol

					// wait for connection from coordinator
					lnr := (ln).(*net.TCPListener)
					defer lnr.SetDeadline(time.Time{})
				startPrecommit:
					lnr.SetDeadline(time.Now().Add(TIMEOUT * time.Duration(NUM_PROCS)))
					conn, err := lnr.Accept()
					if netErr := err.(net.Error); netErr.Timeout() {
						elected, participants := initiateElectionProtocol()
						if elected {
							addTerminationProtocolCoordinator(participants, song, url)
						} else {
							goto startPrecommit
						}
					}

					addTerminationProtocolParticipant(conn, song, url)
				}
				return
			} else if err != nil {
				Error(err)
				return
			}

			if msg == "commit" {
				// write commit record in DT log
				writeToDtLog("commit add", song, url)

				// add song to local playlist
				LocalPlaylist.AddOrUpdateSong(song, url)
			} else {
				Error("coordinator did not respond commit: ", msg)
			}
		} else if msg == "abort" {
			// write abort record in DT log
			writeToDtLog("abort add", song, url)
		} else {
			Error("unrecognized response from coordinator: ", msg)
			return
		}
	} else {
		// vote yes
		fmt.Fprintln(conn, "no")

		// write abort record in DT log
		writeToDtLog("abort add", song, url)
	}
}

func deleteParticipant(ln net.Listener, conn net.Conn, song string) {
	// write yes record in DT log
	writeToDtLog("yes delete", song)

	// vote yes
	fmt.Fprintln(conn, "yes")

	// wait for message from coordinator
	msg, err, timeout := waitForMessageFromCoordinator(conn)
	if timeout {
		elected, participants := initiateElectionProtocol()
		if elected {
			// invoke coordinator's algorithm of
			// termination protocol
			deleteTerminationProtocolCoordinator(participants, song)
		} else {
			// invoke participant's algorithm of
			// termination protocol

			// wait for connection from coordinator
			lnr := (ln).(*net.TCPListener)
			defer lnr.SetDeadline(time.Time{})
		startYes:
			lnr.SetDeadline(time.Now().Add(TIMEOUT * time.Duration(NUM_PROCS)))
			conn, err := lnr.Accept()
			if netErr := err.(net.Error); netErr.Timeout() {
				elected, participants := initiateElectionProtocol()
				if elected {
					deleteTerminationProtocolCoordinator(participants, song)
				} else {
					goto startYes
				}
			}

			deleteTerminationProtocolParticipant(conn, song)
		}
		return
	} else if err != nil {
		Error(err)
		return
	}

	if msg == "pre-commit" {
		// write pre-commit record in DT log
		writeToDtLog("pre-commit delete", song)

		// send ack to coordinator
		fmt.Fprintln(conn, "ack")

		// wait for commit from coordinator
		msg, _, timeout := waitForMessageFromCoordinator(conn)
		if timeout {
			elected, participants := initiateElectionProtocol()
			if elected {
				// invoke coordinator's algorithm of
				// termination protocol
				deleteTerminationProtocolCoordinator(participants, song)
			} else {
				// invoke participant's algorithm of
				// termination protocol

				// wait for connection from coordinator
				lnr := (ln).(*net.TCPListener)
				defer lnr.SetDeadline(time.Time{})
			startPrecommit:
				lnr.SetDeadline(time.Now().Add(TIMEOUT * time.Duration(NUM_PROCS)))
				conn, err := lnr.Accept()
				if netErr := err.(net.Error); netErr.Timeout() {
					elected, participants := initiateElectionProtocol()
					if elected {
						deleteTerminationProtocolCoordinator(participants, song)
					} else {
						goto startPrecommit
					}
				}

				deleteTerminationProtocolParticipant(conn, song)
			}
			return
		}

		if msg == "commit" {
			// write commit record in DT log
			writeToDtLog("commit delete", song)

			// delete song from local playlist
			LocalPlaylist.DeleteSong(song)
		} else {
			Error("coordinator did not respond commit: ", msg)
		}
	} else if msg == "abort" {
		// write abort record in DT log
		writeToDtLog("abort delete", song)
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
			return response, err, true
		} else {
			Error(err)
			return response, err, false
		}
	}

	return strings.TrimSpace(response), nil, false
}

func initiateElectionProtocol() (elected bool, participants []int) {
	alive := LastTimestamp.GetAlive(time.Now())
	for ; len(alive) > 0 && COORDINATOR >= alive[0]; alive = alive[1:] {
	}
	if len(alive) > 0 {
		COORDINATOR = alive[0]
		participants = alive[1:]
	} else {
		COORDINATOR = ID
	}

	if COORDINATOR == ID {
		// tell the master that this server is the coordinator
		MessagesToMaster.Enqueue("coordinator " + strconv.Itoa(ID))
		elected = true
	}

	return elected, participants
}

func addTerminationProtocolCoordinator(participants []int, song, url string) {
	// send STATE-REQ to all participants
	// AND wait for state report messages
	resps := broadcastToParticipantsAndAwaitResponsesTermination(
		participants, fmt.Sprintf("state-req add %s %s", song, url))
	terminationProtocolCoordinatorBody(resps, song, "add "+song+" "+url)
}

func deleteTerminationProtocolCoordinator(participants []int, song string) {
	// send STATE-REQ to all participants
	// AND wait for state report messages
	resps := broadcastToParticipantsAndAwaitResponsesTermination(
		participants, fmt.Sprintf("state-req delete %s", song))
	terminationProtocolCoordinatorBody(resps, song, "delete "+song)
}

func addTerminationProtocolParticipant(conn net.Conn, song, url string) {
	c := bufio.NewReader(conn)
start:
	// wait for state-req from coordinator
	conn.SetDeadline(time.Now().Add(TIMEOUT * time.Duration(NUM_PROCS)))
	_, err := c.ReadString('\n')
	if netErr := err.(net.Error); netErr.Timeout() {
		elected, participants := initiateElectionProtocol()
		if elected {
			addTerminationProtocolCoordinator(participants, song, url)
		} else {
			goto start
		}
	}

	var state string
	_, decision := readVoteOrDecisionFromLog(song)
	if decision == "" || decision == "abort" {
		state = "abort"
	} else if decision == "commit" {
		state = "commit"
	} else if decision == "pre-commit" {
		state = "pre-commit"
	} else {
		state = "uncertain"
	}

	// send state to coordinator
	fmt.Fprintln(conn, state)

	// wait for response from coordinator
	conn.SetDeadline(time.Now().Add(TIMEOUT * time.Duration(NUM_PROCS)))
	resp, err := c.ReadString('\n')
	if netErr := err.(net.Error); netErr.Timeout() {
		elected, participants := initiateElectionProtocol()
		if elected {
			addTerminationProtocolCoordinator(participants, song, url)
		} else {
			goto start
		}
	}

	switch resp {
	case "abort":
		if decision == "" {
			writeToDtLog("abort add", song, url)
		}
	case "commit":
		if decision == "" {
			writeToDtLog("commit add", song, url)
		}
	default:
		// response was pre-commit

		// send ack to coordinator
		fmt.Fprintln(conn, "ack")

		// wait for commit from coordinator
		conn.SetDeadline(time.Now().Add(TIMEOUT * time.Duration(NUM_PROCS)))
		resp, err := c.ReadString('\n')
		if netErr := err.(net.Error); netErr.Timeout() {
			elected, participants := initiateElectionProtocol()
			if elected {
				addTerminationProtocolCoordinator(participants, song, url)
			} else {
				goto start
			}
		}
		if resp != "commit" {
			Error("coordinator responded with \"", resp, "\" instead of 'commit'")
		}

		writeToDtLog("commit add", song, url)
	}
}

func deleteTerminationProtocolParticipant(conn net.Conn, song string) {
	c := bufio.NewReader(conn)
start:
	// wait for state-req from coordinator
	conn.SetDeadline(time.Now().Add(TIMEOUT * time.Duration(NUM_PROCS)))
	_, err := c.ReadString('\n')
	if netErr := err.(net.Error); netErr.Timeout() {
		elected, participants := initiateElectionProtocol()
		if elected {
			deleteTerminationProtocolCoordinator(participants, song)
		} else {
			goto start
		}
	}

	var state string
	_, decision := readVoteOrDecisionFromLog(song)
	if decision == "" || decision == "abort" {
		state = "abort"
	} else if decision == "commit" {
		state = "commit"
	} else if decision == "pre-commit" {
		state = "pre-commit"
	} else {
		state = "uncertain"
	}

	// send state to coordinator
	fmt.Fprintln(conn, state)

	// wait for response from coordinator
	conn.SetDeadline(time.Now().Add(TIMEOUT * time.Duration(NUM_PROCS)))
	resp, err := c.ReadString('\n')
	if netErr := err.(net.Error); netErr.Timeout() {
		elected, participants := initiateElectionProtocol()
		if elected {
			deleteTerminationProtocolCoordinator(participants, song)
		} else {
			goto start
		}
	}

	switch resp {
	case "abort":
		if decision == "" {
			writeToDtLog("abort delete", song)
		}
	case "commit":
		if decision == "" {
			writeToDtLog("commit delete", song)
		}
	default:
		// response was pre-commit

		// send ack to coordinator
		fmt.Fprintln(conn, "ack")

		// wait for commit from coordinator
		conn.SetDeadline(time.Now().Add(TIMEOUT * time.Duration(NUM_PROCS)))
		resp, err := c.ReadString('\n')
		if netErr := err.(net.Error); netErr.Timeout() {
			elected, participants := initiateElectionProtocol()
			if elected {
				deleteTerminationProtocolCoordinator(participants, song)
			} else {
				goto start
			}
		}
		if resp != "commit" {
			Error("coordinator responded with \"", resp, "\" instead of 'commit'")
		}

		writeToDtLog("commit delete", song)
	}
}

func terminationProtocolCoordinatorBody(resps []response, song, operation string) {
	// check for decisions from participants
	anyAborted := false
	anyCommitted := false
	allUncertain := true
	for _, resp := range resps {
		switch resp.v {
		case "abort":
			anyAborted = true
			allUncertain = false
			break
		case "commit":
			anyCommitted = true
			allUncertain = false
			break
		case "uncertain":
			// noop
		default:
			allUncertain = false
		}
	}

	vote, decision := readVoteOrDecisionFromLog(song)
	if coordAborted := decision == "abort"; anyAborted || coordAborted {
		// case TR1
		if !coordAborted {
			writeToDtLog("abort", operation)
		}
		sendToParticipants(resps, "abort")
	} else if coordCommitted := decision == "commit"; anyCommitted || coordCommitted {
		// case TR2
		if !coordCommitted {
			writeToDtLog("commit", operation)
		}
		sendToParticipants(resps, "commit")
	} else if iAmUncertain := vote == "yes"; allUncertain && iAmUncertain {
		// case TR3
		writeToDtLog("abort", operation)
		sendToParticipants(resps, "abort")
	} else {
		// some processes are Commitable - case TR4
		sendToUncertainParticipantsAndAwaitAcks(resps, "pre-commit")
		writeToDtLog("commit", operation)
		sendToUncertainParticipants(resps, "commit")
	}
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

// append a given line to the log
func writeToDtLog(entry ...interface{}) {
	file, err := os.OpenFile(DT_LOG, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	defer file.Close()
	if err != nil {
		Error(err)
		return
	}

	fmt.Fprintln(file, entry...)
}

// returns the most recent vote or decision corresponding to the given song
//
// the following values are possible:
//  vote:	"" (no vote found), "yes"
//  decision:	"" (no decision found), "commit", "abort", "pre-commit"
func readVoteOrDecisionFromLog(song string) (vote, decision string) {
	log, err := ioutil.ReadFile(DT_LOG)
	if err != nil {
		return
	}

	lines := bytes.Split(log, []byte{'\n'})
	songBytes := []byte(song)
	if len(lines) == 0 {
		return
	}
	for i := len(lines) - 1; i >= 0; i-- {
		// check to see if the song is the same as in the operation
		// then set vote or decision accordingly
		args := bytes.Split(lines[i], []byte{' '})
		if len(args) < 3 {
			continue
		}
		logSong := args[2]
		if bytes.Equal(logSong, songBytes) {
			switch string(args[0]) {
			case "start-3pc":
				// I was the coordinator, I neither voted nor
				// made a decision
				return
			case "commit":
				// I committed
				decision = "commit"
				return
			case "abort":
				// I aborted
				decision = "abort"
				return
			case "yes":
				// I am Uncertain
				vote = "yes"
				return
			case "pre-commit":
				// I am Commitable
				decision = "pre-commit"
				return
			}
		}
	}

	return
}
