package main

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
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
		if argLengthAtLeast(2) {
			deleteCoordinator(args[1:])
		}
	case "add":
		if argLengthAtLeast(3) {
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
func addCoordinator(args []string)    {}
func deleteCoordinator(args []string) {}

///////////////////////////////////////////////////////////////////////////////
// participant								     //
///////////////////////////////////////////////////////////////////////////////

func getParticipant(conn net.Conn, song string) {
	url := LocalPlaylist.GetSongUrl(song)
	fmt.Fprintln(conn, "resp", url)
}

func addParticipant(song, url string) {
	LocalPlaylist.AddOrUpdateSong(song, url)
}

func deleteParticipant(song, url string) {
	LocalPlaylist.DeleteSong(song, url)
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
