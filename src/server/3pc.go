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

	case "crashVoteREQ":
		crashVoteREQ(args[1:])
	case "crashPartialPreCommit":
		crashPartialPreCommit(args[1:])
	case "crashPartialCommit":
		crashPartialCommit(args[1:])
	default:
		Error("unrecognized command: \"", command, "\"")
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

			// TODO: REMOVE
			fmt.Println(ID, LastTimestamp.GetAlive(time.Now()))
			resp, err := sendAndWaitForResponse(mJson, id)
			if err != nil {
				continue
			}

			args := strings.Split(string(resp), " ")
			if len(args) >= 2 {
				if args[0] == "resp" && args[1] != "NONE" {
					url = string(args[1])
					LocalPlaylist.AddOrUpdateSong(song, url)
					// TODO: DELETE?
					//
					// MAYBE because you might get a value
					// that is about to be removed in a
					// current commit and then you'll
					// create an inconsistent state
					writeToDtLog("commit add", song, url)
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
	resps, err := sendVoteREQToParticipantsAndAwaitResponses(
		fmt.Sprintf("vote-req add %s %s", song, url))
	defer func() {
		for _, resp := range resps {
			resp.c.Close()
		}
	}()
	if err != nil {
		// write abort record in DT log
		writeToDtLog("abort add", song, url)

		// send abort to all processes that voted yes
		sendAbortToYesVoters(resps)

		// send abort to master
		MessagesToMaster.Enqueue("ack abort")
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
		sendToParticipantsAndAwaitAcks(resps, "commit")

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
}

// TODO
func deleteCoordinator(args []string) {
	song := args[0]

	// write start-3pc record in DT log
	writeToDtLog("start-3pc delete", song)

	// send VOTE-REQ to all participants
	// AND wait for vote messages from all participants
	resps, err := sendVoteREQToParticipantsAndAwaitResponses(
		"vote-req delete " + song)
	defer func() {
		for _, resp := range resps {
			resp.c.Close()
		}
	}()
	if err != nil {
		// write abort record in DT log
		writeToDtLog("abort delete", song)

		// send abort to all processes that voted yes
		sendAbortToYesVoters(resps)

		// send abort to master
		MessagesToMaster.Enqueue("ack abort")
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
		sendToParticipantsAndAwaitAcks(resps, "commit")

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
	resps, err := sendVoteREQToParticipantsAndAwaitResponses(
		fmt.Sprintf("vote-req %s", operation))
	defer func() {
		for _, resp := range resps {
			resp.c.Close()
		}
	}()
	if err != nil {
		// write abort record in DT log
		writeToDtLog("abort", update)

		// send abort to all processes that voted yes
		sendAbortToYesVoters(resps)

		// send abort to master
		MessagesToMaster.Enqueue("ack abort")
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
		writeToDtLog("commit", update)

		// send commit to all participants
		sendToParticipantsAndAwaitAcks(resps, "commit")

		// send abort to master
		MessagesToMaster.Enqueue("ack commit")

		// add song to local playlist
		LocalPlaylist.AddOrUpdateSong(song, url)
	} else {
		// some participant voted no

		// write abort record in DT log
		writeToDtLog("abort", update)

		// send abort to all processes that voted yes
		sendAbortToYesVoters(resps)

		// send abort to master
		MessagesToMaster.Enqueue("ack abort")
	}

	return
}

func sendVoteREQToParticipantsAndAwaitResponses(msg string) ([]response, error) {
	type connection struct {
		c  net.Conn
		id int
	}

	var responses []response
	var conns []connection

	msgBytes, err := json.Marshal(newMessage(msg))
	if err != nil {
		return nil, err
	}
	msgJSON := string(msgBytes)

	// send message to participants
	participants := LastTimestamp.GetAlive(time.Now())
	// TODO: REMOVE
	fmt.Println("broadcast participants:", participants)
	if CrashVoteREQ && len(CrashVoteREQList) == 0 {
		Fatal(ID, "CrashVoteREQ")
	}
	for _, id := range participants {
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

		if CrashVoteREQ {
			for i, tid := range CrashVoteREQList {
				if id == tid {
					copy(CrashVoteREQList[i:], CrashVoteREQList[i+1:])
					CrashVoteREQList = CrashVoteREQList[:len(CrashVoteREQList)-1]
					break
				}
			}

			if len(CrashVoteREQList) == 0 {
				Fatal(ID, "CrashVoteREQ")
			}
		}
	}

	// wait for responses from all recipients
	for _, conn := range conns {
		r := bufio.NewReader(conn.c)

		var resp string
		resp, err = r.ReadString('\n')
		resp = strings.TrimSpace(resp)
		if err == nil {
			responses = append(responses, response{resp, conn.c, conn.id})
		}
	}

	return responses, err
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
			if err == nil {
				fmt.Fprintln(conn, abortJson)
			}
		}
	}
}

func sendToParticipantsAndAwaitAcks(participants []response, msg string) {
	if msg == "pre-commit" && CrashPartialPreCommit && len(CrashPartialPreCommitList) == 0 {
		Fatal(ID, "CrashPartialPreCommit")
	} else if msg == "commit" && CrashPartialCommit && len(CrashPartialCommitList) == 0 {
		Fatal(ID, "CrashPartialCommit")
	}

	// send message to participants
	for i, ptc := range participants {
		if ptc.id == ID {
			continue
		}

		_, err := fmt.Fprintln(ptc.c, msg)
		if err != nil {
			participants[i].c = nil
		}

		if msg == "pre-commit" && CrashPartialPreCommit {
			for j, tid := range CrashPartialPreCommitList {
				if ptc.id == tid {
					copy(CrashPartialPreCommitList[j:], CrashPartialPreCommitList[j+1:])
					CrashPartialPreCommitList = CrashPartialPreCommitList[:len(CrashPartialPreCommitList)-1]
					break
				}
			}

			if len(CrashPartialPreCommitList) == 0 {
				Fatal(ID, "CrashPartialPreCommit")
			}
		} else if msg == "commit" && CrashPartialCommit {
			for j, tid := range CrashPartialCommitList {
				if ptc.id == tid {
					copy(CrashPartialCommitList[j:], CrashPartialCommitList[j+1:])
					CrashPartialCommitList = CrashPartialCommitList[:len(CrashPartialCommitList)-1]
					break
				}
			}

			if len(CrashPartialCommitList) == 0 {
				Fatal(ID, "CrashPartialCommit")
			}
		}
	}

	// wait for responses from all recipients
	for _, ptc := range participants {
		if ptc.c != nil {
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

func addParticipant(ln net.Listener, conn net.Conn, song, url string) {
	vote := vote(url)
	if vote == "yes" {
		// write yes record in DT log
		writeToDtLog("yes add", song, url)
		if CrashBeforeVote {
			Fatal(ID, "CrashBeforeVote")
		}

		// vote yes
		fmt.Fprintln(conn, "yes")
		if CrashAfterVote {
			Fatal(ID, "CrashAfterVote")
		}

		// wait for message from coordinator
		rdr := bufio.NewReader(conn)
		msg, err := rdr.ReadString('\n')
		msg = strings.TrimSpace(msg)
		if err != nil {
			initiateElectionAndTerminationProtocol(ln, song, fmt.Sprintf("add %s %s", song, url),
				func(ptcps []int) {
					addTerminationProtocolCoordinator(ptcps, song, url)
				})
			return
		}

		if msg == "pre-commit" {
			// write pre-commit record in DT log
			writeToDtLog("pre-commit add", song, url)

			// send ack to coordinator
			fmt.Fprintln(conn, "ack")
			if CrashAfterAck {
				Fatal(ID, "CrashAfterAck")
			}

			// wait for commit from coordinator
			msg, err := rdr.ReadString('\n')
			msg = strings.TrimSpace(msg)
			if err != nil {
				initiateElectionAndTerminationProtocol(ln, song, fmt.Sprintf("add %s %s", song, url),
					func(ptcps []int) {
						addTerminationProtocolCoordinator(ptcps, song, url)
					})
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
	if CrashBeforeVote {
		Fatal(ID, "CrashBeforeVote")
	}

	// vote yes
	fmt.Fprintln(conn, "yes")
	if CrashAfterVote {
		Fatal(ID, "CrashAfterVote")
	}

	// wait for message from coordinator
	rdr := bufio.NewReader(conn)
	msg, err := rdr.ReadString('\n')
	msg = strings.TrimSpace(msg)
	if err != nil {
		initiateElectionAndTerminationProtocol(ln, song, fmt.Sprintf("delete %s", song),
			func(ptcps []int) {
				deleteTerminationProtocolCoordinator(ptcps, song)
			})
		return
	}

	if msg == "pre-commit" {
		// write pre-commit record in DT log
		writeToDtLog("pre-commit delete", song)

		// send ack to coordinator
		fmt.Fprintln(conn, "ack")
		if CrashAfterAck {
			Fatal(ID, "CrashAfterAck")
		}

		// wait for commit from coordinator
		msg, err := rdr.ReadString('\n')
		msg = strings.TrimSpace(msg)
		if err != nil {
			initiateElectionAndTerminationProtocol(ln, song, fmt.Sprintf("delete %s", song),
				func(ptcps []int) {
					deleteTerminationProtocolCoordinator(ptcps, song)
				})
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
	response, err := r.ReadString('\n')
	if err != nil {
		Error("timed out when reading response from coordinator")
		return "", err, true
	}

	return strings.TrimSpace(response), nil, false
}

///////////////////////////////////////////////////////////////////////////////
// election protocol                                                         //
///////////////////////////////////////////////////////////////////////////////

func initiateElectionProtocol(ptcps []int) (elected bool, participants []int) {
	// TODO: REMOVE
	fmt.Println(ID, "electing a new coordinator; was", COORDINATOR)
	for ; len(ptcps) > 0 && COORDINATOR >= ptcps[0]; ptcps = ptcps[1:] {
	}
	if len(ptcps) > 0 {
		COORDINATOR = ptcps[0]
		participants = ptcps[1:]
	} else {
		COORDINATOR = ID
	}

	// TODO: REMOVE
	fmt.Print("\033[33m")
	fmt.Println(ID, "elected", COORDINATOR)
	fmt.Print("\033[0m")

	if COORDINATOR == ID {
		// tell the master that this server is the coordinator
		MessagesToMaster.Enqueue("coordinator " + strconv.Itoa(ID))
		elected = true
	}

	return elected, participants
}

func initiateElectionAndTerminationProtocol(ln net.Listener, song, operation string, terminationCoordinator func(ptcps []int)) {
	elected, participants := initiateElectionProtocol(LastTimestamp.GetAlive(time.Now()))
	// TODO: REMOVE
	fmt.Println(ID, "initiateElectionAndTerminationProtocol", operation)
	fmt.Println(ID, "elected:", elected, "participants", participants)
	if elected {
		terminationCoordinator(participants)
	} else {
		// TODO: REMOVE?
		lnr := (ln).(*net.TCPListener)
		//defer lnr.SetDeadline(time.Time{})

	start:
		// wait for state-req from coordinator
		//lnr.SetDeadline(time.Now().Add(TIMEOUT * time.Duration(NUM_PROCS)))
		conn, err := lnr.Accept()
		// TODO: FIXERUP
		if err != nil {
			elected, participants := initiateElectionProtocol(participants)
			if elected {
				terminationCoordinator(participants)
			} else {
				goto start
			}
		}

		// read state-req from coordinator
		c := bufio.NewReader(conn)
		req, err := c.ReadString('\n')
		req = strings.TrimSpace(req)
		if err != nil {
			elected, participants := initiateElectionProtocol(participants)
			if elected {
				terminationCoordinator(participants)
			} else {
				goto start
			}
		}

		gotoStart := terminationProtocolParticipantBody(
			conn, c, req, song, operation, participants, terminationCoordinator)
		if gotoStart {
			goto start
		}
	}
}

func handleStateReq(ln net.Listener, conn net.Conn, req string, args []string) {
	// TODO: REMOVE?
	lnr := (ln).(*net.TCPListener)
	//defer lnr.SetDeadline(time.Time{})

	var terminationCoordinator func(ptcps []int)
	song := args[2]
	operation := strings.Join(args[1:], " ")
	switch args[1] {
	case "add":
		url := args[3]
		terminationCoordinator = func(ptcps []int) {
			addTerminationProtocolCoordinator(ptcps, song, url)
		}
	case "delete":
		terminationCoordinator = func(ptcps []int) {
			deleteTerminationProtocolCoordinator(ptcps, song)
		}
	default:
		Fatal("invalid command", args)
	}

	c := bufio.NewReader(conn)
	participants := LastTimestamp.GetAlive(time.Now())
	gotoStart := terminationProtocolParticipantBody(
		conn, c, req, song, operation, participants, terminationCoordinator)
	if !gotoStart {
		return
	}

start:
	// wait for state-req from coordinator
	//lnr.SetDeadline(time.Now().Add(TIMEOUT * time.Duration(NUM_PROCS)))
	conn.Close()
	conn, err := lnr.Accept()
	// TODO: FIXERUP
	if err != nil {
		elected, participants := initiateElectionProtocol(participants)
		if elected {
			terminationCoordinator(participants)
		} else {
			goto start
		}
	}

	// read state-req from coordinator
	c = bufio.NewReader(conn)
	req, err = c.ReadString('\n')
	req = strings.TrimSpace(req)
	if err != nil {
		elected, participants := initiateElectionProtocol(participants)
		if elected {
			terminationCoordinator(participants)
		} else {
			goto start
		}
	}

	gotoStart = terminationProtocolParticipantBody(
		conn, c, req, song, operation, participants, terminationCoordinator)
	if gotoStart {
		goto start
	}
}

func terminationProtocolParticipantBody(conn net.Conn, c *bufio.Reader, req, song,
	operation string, participants []int, terminationCoordinator func(ptcps []int)) (gotoStart bool) {

	// TODO: REMOVE
	req = strings.TrimSpace(req)
	if strings.Split(req, " ")[0] != "state-req" {
		Error("participant did not receive expected state-req: ", req)
	}

	var state string
	vote, decision := readVoteOrDecisionFromLog(song)
	if decision == "commit" {
		state = "commit"
	} else if decision == "pre-commit" {
		state = "pre-commit"
	} else if decision == "abort" || vote == "" {
		state = "abort"
	} else {
		state = "uncertain"
	}

	// send state to coordinator
	// TODO: REMOVE
	fmt.Println("termination participant", ID, "sent", state)
	_, err := fmt.Fprintln(conn, state)
	if err != nil {
		elected, participants := initiateElectionProtocol(participants)
		if elected {
			terminationCoordinator(participants)
		} else {
			gotoStart = true
			return
		}
	}

	// wait for response from coordinator
	resp, err := c.ReadString('\n')
	resp = strings.TrimSpace(resp)
	if err != nil {
		elected, participants := initiateElectionProtocol(participants)
		if elected {
			terminationCoordinator(participants)
		} else {
			gotoStart = true
			return
		}
	}

	// TODO: REMOVE
	fmt.Println(ID, "received", resp, "from coordinator")
	switch resp {
	case "abort":
		if noAbortInDtLog := decision == ""; noAbortInDtLog {
			// TODO: REMOVE
			fmt.Println(ID, "writing abort to log")
			writeToDtLog("abort", operation)
		}
	case "commit":
		if noCommitInDtLog := decision == ""; noCommitInDtLog {
			writeToDtLog("commit", operation)
			args := strings.Split(operation, " ")
			switch args[0] {
			case "add":
				LocalPlaylist.AddOrUpdateSong(args[1], args[2])
			case "delete":
				LocalPlaylist.DeleteSong(args[1])
			}
		}
	default:
		// response was pre-commit

		// send ack to coordinator
		fmt.Fprintln(conn, "ack")

		// wait for commit from coordinator
		resp, err := c.ReadString('\n')
		resp = strings.TrimSpace(resp)
		if err != nil {
			elected, participants := initiateElectionProtocol(participants)
			if elected {
				terminationCoordinator(participants)
			} else {
				gotoStart = true
				return
			}
		}
		if resp != "commit" {
			Error("coordinator responded with \"", resp, "\" instead of 'commit'")
		}

		// TODO: REMOVE
		fmt.Println(ID, "writing commit to log")
		writeToDtLog("commit", operation)
		args := strings.Split(operation, " ")
		switch args[0] {
		case "add":
			LocalPlaylist.AddOrUpdateSong(args[1], args[2])
		case "delete":
			LocalPlaylist.DeleteSong(args[1])
		}
	}

	return
}

///////////////////////////////////////////////////////////////////////////////
// termination protocol coordinator                                          //
///////////////////////////////////////////////////////////////////////////////

// TODO: REVIEW
func addTerminationProtocolCoordinator(participants []int, song, url string) {
	// send STATE-REQ to all participants
	// AND wait for state report messages
	operation := fmt.Sprintf("add %s %s", song, url)
	// TODO: REMOVE
	fmt.Println(ID, "sending state-reqs")
	resps := broadcastToParticipantsAndAwaitResponsesTermination(
		participants, "state-req "+operation)
	// TODO: REMOVE
	fmt.Println(ID, "responses", resps)
	defer func() {
		for _, resp := range resps {
			resp.c.Close()
		}
	}()
	terminationProtocolCoordinatorBody(resps, song, operation)
}

// TODO: REVIEW
func deleteTerminationProtocolCoordinator(participants []int, song string) {
	// send STATE-REQ to all participants
	// AND wait for state report messages
	operation := "delete " + song
	resps := broadcastToParticipantsAndAwaitResponsesTermination(
		participants, "state-req "+operation)
	defer func() {
		for _, resp := range resps {
			resp.c.Close()
		}
	}()
	terminationProtocolCoordinatorBody(resps, song, operation)
}

// TODO: REVIEW
func broadcastToParticipantsAndAwaitResponsesTermination(participants []int, msg string) []response {
	type connection struct {
		c  net.Conn
		id int
	}
	var responses []response
	var conns []connection

	// send message to participants
	for _, id := range participants {
		if id == ID {
			continue
		}

		conn, err := net.Dial("tcp", ":"+strconv.Itoa(START_PORT+id))
		if err != nil {
			continue
		}

		_, err = fmt.Fprintln(conn, msg)
		if err == nil {
			conns = append(conns, connection{conn, id})
		} else {
			conn.Close()
		}
	}

	// wait for responses from all recipients
	for _, conn := range conns {
		r := bufio.NewReader(conn.c)
		resp, err := r.ReadString('\n')
		if err != nil {
			conn.c.Close()
			continue
		}

		resp = strings.TrimSpace(resp)
		responses = append(responses, response{resp, conn.c, conn.id})
	}

	return responses
}

// TODO: REVIEW
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
		// TODO: REMOVE
		fmt.Println("coordinator sending aborts to participants", resps)
		sendToParticipants(resps, "abort")
	} else if coordCommitted := decision == "commit"; anyCommitted || coordCommitted {
		// case TR2
		if !coordCommitted {
			writeToDtLog("commit", operation)
			args := strings.Split(operation, " ")
			switch args[0] {
			case "add":
				LocalPlaylist.AddOrUpdateSong(args[1], args[2])
			case "delete":
				LocalPlaylist.DeleteSong(args[1])
			}
		}
		sendToParticipants(resps, "commit")
	} else if iAmUncertain := vote == "yes"; allUncertain && iAmUncertain {
		// case TR3
		// TODO: REMOVE
		fmt.Println("coordinator voted yes; sending aborts to participants")
		writeToDtLog("abort", operation)
		sendToParticipants(resps, "abort")
	} else {
		// some processes are Commitable - case TR4
		sendToUncertainParticipantsAndAwaitAcks(resps, "pre-commit")
		writeToDtLog("commit", operation)
		args := strings.Split(operation, " ")
		switch args[0] {
		case "add":
			LocalPlaylist.AddOrUpdateSong(args[1], args[2])
		case "delete":
			LocalPlaylist.DeleteSong(args[1])
		}
		sendToUncertainParticipants(resps, "commit")
	}
}

// TODO: REVIEW
func sendToParticipants(participants []response, msg string) {
	// send message to participants
	for i, ptc := range participants {
		_, err := fmt.Fprintln(ptc.c, msg)
		if err != nil {
			participants[i].c = nil
		}
	}
}

// TODO: REVIEW
func sendToUncertainParticipantsAndAwaitAcks(participants []response, msg string) {
	// send message to participants
	sendToUncertainParticipants(participants, msg)

	// wait for responses from all recipients
	for _, ptc := range participants {
		if ptc.c != nil && ptc.v == "uncertain" {
			r := bufio.NewReader(ptc.c)

			// read ack from recipient
			r.ReadString('\n')
		}
	}
}

// TODO: REVIEW
func sendToUncertainParticipants(participants []response, msg string) {
	// send message to participants
	for i, resp := range participants {
		if resp.v == "uncertain" {
			_, err := fmt.Fprintln(resp.c, msg)
			if err != nil {
				participants[i].c = nil
			}
		}
	}
}

///////////////////////////////////////////////////////////////////////////////
// crash                                                                     //
///////////////////////////////////////////////////////////////////////////////

func crash() {
	Fatal("Crash")
}

func crashAfterVote() {
	CrashAfterVote = true
}

func crashBeforeVote() {
	CrashBeforeVote = true
}

func crashAfterAck() {
	CrashAfterAck = true
}

func crashVoteREQ(args []string) {
	CrashVoteREQ = true
	for _, s := range args {
		i, err := strconv.Atoi(s)
		if err == nil && i != ID {
			CrashVoteREQList = append(CrashVoteREQList, i)
		}
	}
}

func crashPartialPreCommit(args []string) {
	CrashPartialPreCommit = true
	for _, s := range args {
		i, err := strconv.Atoi(s)
		if err == nil && i != ID {
			CrashPartialPreCommitList = append(CrashPartialPreCommitList, i)
		}
	}
}

func crashPartialCommit(args []string) {
	CrashPartialCommit = true
	for _, s := range args {
		i, err := strconv.Atoi(s)
		if err == nil && i != ID {
			CrashPartialCommitList = append(CrashPartialCommitList, i)
		}
	}
}

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

// TODO: REVIEW
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
