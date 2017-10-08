package main

import (
	"bufio"
	"strconv"
	"sync"
	"time"
)

type tsTimestampQueue struct {
	value []time.Time
	mutex sync.Mutex // mutex for accessing contents
}

// NOTE: assumes message IDs are in {0..n-1}
func (tsq *tsTimestampQueue) UpdateTimestamp(msg *Message) {
	tsq.mutex.Lock()
	defer tsq.mutex.Unlock()
	tsq.value[msg.Id] = msg.Rts
}

func (tsq *tsTimestampQueue) UpdateTimestampById(id int) {
	tsq.mutex.Lock()
	defer tsq.mutex.Unlock()
	tsq.value[id] = time.Now()
}

func (tsq *tsTimestampQueue) WriteAlive(rwr *bufio.ReadWriter, now time.Time) {
	tsq.mutex.Lock()
	defer tsq.mutex.Unlock()
	stmps := tsq.value
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

func (tsq *tsTimestampQueue) GetAlive(now time.Time) []int {
	var alive []int

	tsq.mutex.Lock()
	defer tsq.mutex.Unlock()
	stmps := tsq.value
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

	return alive
}

func (tsq *tsTimestampQueue) IsAlive(id int) bool {
	tsq.mutex.Lock()
	defer tsq.mutex.Unlock()
	if id != -1 && time.Now().Sub(tsq.value[id]) < ALIVE_INTERVAL {
		return true
	}
	return false
}

func (tsq *tsTimestampQueue) LowestIdAlive() int {
	now := time.Now()

	tsq.mutex.Lock()
	defer tsq.mutex.Unlock()
	stmps := tsq.value
	for id := 0; id < ID; id++ {
		// add all server ids for which a
		// heartbeat was sent within the
		// alive interval
		if now.Sub(stmps[id]) < ALIVE_INTERVAL {
			return id
		}
	}

	return ID
}

type tsStringQueue struct {
	value []string
	mutex sync.Mutex // mutex for accessing contents
}

func (tsq *tsStringQueue) Enqueue(v string) {
	tsq.mutex.Lock()
	defer tsq.mutex.Unlock()
	tsq.value = append(tsq.value, v)
}

func (tsq *tsStringQueue) PushFront(v string) {
	tsq.mutex.Lock()
	defer tsq.mutex.Unlock()
	tsq.value = append([]string{v}, tsq.value...)
}

func (tsq *tsStringQueue) Dequeue() string {
	tsq.mutex.Lock()
	defer tsq.mutex.Unlock()
	var v string
	if len(tsq.value) > 0 {
		v = tsq.value[0]
		tsq.value = tsq.value[1:]
	}
	return v
}
