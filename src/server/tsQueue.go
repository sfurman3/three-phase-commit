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
	{
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
	}
	LastTimestamp.mutex.Unlock()
}
