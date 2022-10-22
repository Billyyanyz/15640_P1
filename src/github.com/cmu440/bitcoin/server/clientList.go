package main

import (
	"fmt"
	"math"
)

type clientRequestStatus struct {
	msg          string
	maxNonce     uint64
	waitList     []uint64 // delete when sending work, adding back when miner crash
	finishedCnt  uint64
	minHash      uint64
	minHashNonce uint64
}

// list of last elements of a chunk, 0-9999=>9999, 10000-12588=>12588
// retrieved x, process (x/10000*10000) to x inclusive
func chunkList(maxNonce uint64) []uint64 {
	n := maxNonce/CHUNKSIZE + 1
	chunks := make([]uint64, n)
	for i, _ := range chunks {
		chunks[i] = CHUNKSIZE - 1 + uint64(i)*CHUNKSIZE
	}
	chunks[n-1] = maxNonce
	return chunks
}

func newClientRequestStatus(msg string, maxNonce uint64) *clientRequestStatus {
	return &clientRequestStatus{
		msg:          msg,
		maxNonce:     maxNonce,
		waitList:     chunkList(maxNonce),
		finishedCnt:  0,
		minHash:      math.MaxUint64,
		minHashNonce: 0,
	}
}

type clientList struct {
	clients        []int
	loopIdx        int
	clientRequests map[int]*clientRequestStatus
}

func (cl *clientList) add(c int, msg string, maxNonce uint64) {
	if _, ok := cl.clientRequests[c]; ok {
		fmt.Println("Adding ", c, " already in client list!")
		return
	}
	cl.clients = append(cl.clients, c)
	cl.clientRequests[c] = newClientRequestStatus(msg, maxNonce)
}

func (cl *clientList) delete(c int) {
	if _, ok := cl.clientRequests[c]; !ok {
		fmt.Println("Deleting ", c, " not in client list!")
		return
	}
	delete(cl.clientRequests, c)
	for i, client := range cl.clients {
		if client == c {
			cl.clients[i] = cl.clients[len(cl.clients)-1]
			cl.clients = cl.clients[:len(cl.clients)-1]
			if cl.loopIdx >= len(cl.clients) {
				cl.loopIdx = 0
			}
		}
	}
}

func (cl *clientList) check(c int) bool {
	_, ok := cl.clientRequests[c]
	return ok
}

func (cl *clientList) checkNext() bool {
	if len(cl.clients) == 0 {
		return false
	}
	start := cl.loopIdx
	for {
		if len(cl.clientRequests[cl.clients[cl.loopIdx]].waitList) != 0 {
			return true
		}
		cl.loopIdx = (cl.loopIdx + 1) % len(cl.clients)
		if cl.loopIdx == start {
			return false
		}
	}
}

func (cl *clientList) getNext() minerWork {
	cID := cl.clients[cl.loopIdx]
	cReq := cl.clientRequests[cID]
	maxNonce := cReq.waitList[len(cReq.waitList)-1]
	cReq.waitList = cReq.waitList[:len(cReq.waitList)-1]
	cl.loopIdx = (cl.loopIdx + 1) % len(cl.clients)
	return minerWork{cID, maxNonce}
}

func (cl *clientList) addBackWork(work minerWork) {
	c := work.clientID
	if cl.check(c) {
		cReq := cl.clientRequests[c]
		cReq.waitList = append(cReq.waitList, work.maxNonce)
	}
}

func (cl *clientList) recvResult(hash uint64, nonce uint64, c int) (bool, uint64, uint64) {
	if cl.check(c) {
		cReq := cl.clientRequests[c]
		if hash < cReq.minHash {
			cReq.minHash = hash
			cReq.minHashNonce = nonce
		}
		cReq.finishedCnt++
		if cReq.finishedCnt == cReq.maxNonce/CHUNKSIZE+1 {
			defer cl.delete(c)
			return true, cReq.minHash, cReq.minHashNonce
		}
	}
	return false, 0, 0
}
