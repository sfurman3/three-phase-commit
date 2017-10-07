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
		conn.c.SetDeadline(time.Now().Add(TIMEOUT))
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
			// TODO: REMOVE
			Error(err, COORDINATOR, msg)
			initiateElectionProtocol()
			return
		}

		if msg == "pre-commit" {
			// send ack to coordinator
			fmt.Fprintln(conn, "ack")
			if CrashAfterAck {
				Fatal(ID, "CrashAfterAck")
			}

			// wait for commit from coordinator
			msg, err := rdr.ReadString('\n')
			msg = strings.TrimSpace(msg)
			if err != nil {
				// TODO: REMOVE
				Error(err, COORDINATOR, msg)
				initiateElectionProtocol()
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

func deleteParticipant(conn net.Conn, song string) {
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
		// TODO: REMOVE
		Error(err, COORDINATOR, msg)
		initiateElectionProtocol()
		return
	}

	if msg == "pre-commit" {
		// send ack to coordinator
		fmt.Fprintln(conn, "ack")
		if CrashAfterAck {
			Fatal(ID, "CrashAfterAck")
		}

		// wait for commit from coordinator
		msg, err := rdr.ReadString('\n')
		msg = strings.TrimSpace(msg)
		if err != nil {
			// TODO: REMOVE
			Error(err, COORDINATOR, msg)
			initiateElectionProtocol()
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
			return "", err, true
		} else {
			Error(err)
			return "", err, false
		}
	}

	return strings.TrimSpace(response), nil, false
}

func initiateElectionProtocol() (elected bool, participants []int) {
	// TODO: REMOVE
	fmt.Println(ID, "electing a new coordinator", COORDINATOR)
	alive := LastTimestamp.GetAlive(time.Now())
	for ; len(alive) > 0 && COORDINATOR >= alive[0]; alive = alive[1:] {
	}
	if len(alive) > 0 {
		COORDINATOR = alive[0]
		participants = alive[1:]
	} else {
		COORDINATOR = ID
	}

	// TODO: REMOVE
	fmt.Println(ID, "elected", COORDINATOR)

	if COORDINATOR == ID {
		// tell the master that this server is the coordinator
		MessagesToMaster.Enqueue("coordinator " + strconv.Itoa(ID))
		elected = true
	}

	return elected, participants
}

///////////////////////////////////////////////////////////////////////////////
// crash      								     //
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
