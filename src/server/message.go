package main

import "time"

// Message represents a message sent from one server to another
type Message struct {
	Id      int       `json:"id"`  // server id
	Rts     time.Time `json:"rts"` // real-time timestamp
	Content string    `json:"msg"` // content of the message
}

// emptyMessage returns an empty message with a timestamp of time.Now()
func emptyMessage() *Message {
	return &Message{
		Id:  ID,
		Rts: time.Now(),
	}
}

// newMessage returns a message with Content msg and a timestamp of time.Now()
func newMessage(msg string) *Message {
	return &Message{
		Id:      ID,
		Rts:     time.Now(),
		Content: msg,
	}
}
