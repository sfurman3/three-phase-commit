package main

import (
	"bufio"
	"strconv"
	"sync"
	"time"
)

type tsMsgQueue struct {
	value []*Message
	mutex sync.Mutex // mutex for accessing contents
}

func (tsq *tsMsgQueue) Enqueue(msg *Message) {
	tsq.mutex.Lock()
	tsq.value = append(tsq.value, msg)
	tsq.mutex.Unlock()
}

func (tsq *tsMsgQueue) Dequeue() *Message {
	tsq.mutex.Lock()
	var v *Message
	if len(tsq.value) > 0 {
		v = tsq.value[0]
		tsq.value = tsq.value[1:]
	}
	tsq.mutex.Unlock()
	return v
}

func (tsq *tsMsgQueue) WriteMessages(rwr *bufio.ReadWriter) {
	tsq.mutex.Lock()
	if len(tsq.value) > 0 {
		msgs := tsq.value
		lst := len(msgs) - 1
		for _, msg := range msgs[:lst] {
			rwr.WriteString(msg.Content)
			rwr.WriteByte(',')
		}
		rwr.WriteString(msgs[lst].Content)
	}
	tsq.mutex.Unlock()
}

type tsTimestampQueue struct {
	value []time.Time
	mutex sync.Mutex // mutex for accessing contents
}

// NOTE: assumes message IDs are in {0..n-1}
func (tsq *tsTimestampQueue) UpdateTimestamp(msg *Message) {
	LastTimestamp.mutex.Lock()
	LastTimestamp.value[msg.Id] = msg.Rts
	LastTimestamp.mutex.Unlock()
}

func (tsq *tsTimestampQueue) WriteAlive(rwr *bufio.ReadWriter, now time.Time) {
	LastTimestamp.mutex.Lock()
	stmps := LastTimestamp.value
	for id := 0; id < ID; id++ {
		// add all server ids for which a
		// heartbeat was sent within the
		// alive interval
		if now.Sub(stmps[id]) < ALIVE_INTERVAL {
			rwr.WriteString(strconv.Itoa(id))
			rwr.WriteByte(',')
		}
	}
	rwr.WriteString(strconv.Itoa(ID))
	for id := ID + 1; id < NUM_PROCS; id++ {
		// add all server ids for which a
		// heartbeat was sent within the
		// heartbeat interval
		if now.Sub(stmps[id]) < HEARTBEAT_INTERVAL {
			rwr.WriteByte(',')
			rwr.WriteString(strconv.Itoa(id))
		}
	}
	LastTimestamp.mutex.Unlock()
}

func (tsq *tsTimestampQueue) GetAlive(now time.Time) []int {
	var alive []int

	LastTimestamp.mutex.Lock()
	stmps := LastTimestamp.value
	for id := 0; id < ID; id++ {
		// add all server ids for which a
		// heartbeat was sent within the
		// alive interval
		if now.Sub(stmps[id]) < ALIVE_INTERVAL {
			alive = append(alive, id)
		}
	}
	alive = append(alive, ID)
	for id := ID + 1; id < NUM_PROCS; id++ {
		// add all server ids for which a
		// heartbeat was sent within the
		// alive interval
		if now.Sub(stmps[id]) < ALIVE_INTERVAL {
			alive = append(alive, id)
		}
	}
	LastTimestamp.mutex.Unlock()

	return alive
}

func (tsq *tsTimestampQueue) LowestIdAlive() int {
	now := time.Now()

	LastTimestamp.mutex.Lock()
	stmps := LastTimestamp.value
	for id := 0; id < ID; id++ {
		// add all server ids for which a
		// heartbeat was sent within the
		// alive interval
		if now.Sub(stmps[id]) < ALIVE_INTERVAL {
			return id
		}
	}
	LastTimestamp.mutex.Unlock()

	return ID
}

type tsStringQueue struct {
	value []string
	mutex sync.Mutex // mutex for accessing contents
}

func (tsq *tsStringQueue) Enqueue(v string) {
	tsq.mutex.Lock()
	tsq.value = append(tsq.value, v)
	tsq.mutex.Unlock()
}

func (tsq *tsStringQueue) PushFront(v string) {
	tsq.mutex.Lock()
	tsq.value = append([]string{v}, tsq.value...)
	tsq.mutex.Unlock()
}

func (tsq *tsStringQueue) Dequeue() string {
	tsq.mutex.Lock()
	var v string
	if len(tsq.value) > 0 {
		v = tsq.value[0]
		tsq.value = tsq.value[1:]
	}
	tsq.mutex.Unlock()
	return v
}
