package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"log"
	"math"
	"os"
	"strconv"
)

const CHUNKSIZE uint64 = 10000

type clientRequestStatus struct {
	msg          string
	maxNonce     uint64
	waitList     []uint64 // delete when sending work, adding back when miner crash
	finishedCnt  uint64
	minHash      uint64
	minHashNonce uint64
}

// list of last elements of a chunk, 0-999=>999, 12000-12588=>12588
// retrieved x, process (x/1000*1000) to x inclusive
func chunkList(maxNonce uint64) []uint64 {
	n := maxNonce/CHUNKSIZE + 1
	chunks := make([]uint64, n)
	for i, _ := range chunks {
		chunks[i] = CHUNKSIZE - 1 + uint64(i)*CHUNKSIZE
	}
	chunks[n-1] = maxNonce
	return chunks
}

func newClientRequestStatus(msg string, maxNonce uint64) clientRequestStatus {
	return clientRequestStatus{
		msg:          msg,
		maxNonce:     maxNonce,
		waitList:     chunkList(maxNonce),
		finishedCnt:  0,
		minHash:      math.MaxUint64,
		minHashNonce: -1,
	}
}

type minerWork struct {
	clientID int
	maxNonce uint64
}

type server struct {
	lspServer lsp.Server

	clients        []int
	cLoopIdx       int
	clientRequests map[int]clientRequestStatus // client id - request

	freeMiners []int
	minerWorks map[int]minerWork // miner id - work, cID=-1 => vacant
}

func startServer(port int) (*server, error) {
	lspServer, err := lsp.NewServer(port, lsp.NewParams())
	return &server{
		lspServer:      lspServer,
		clients:        make([]int, 0, 10),
		cLoopIdx:       -1,
		clientRequests: make(map[int]clientRequestStatus),
		freeMiners:     make([]int, 0, 10),
		minerWorks:     make(map[int]minerWork),
	}, err
}

var LOGF *log.Logger

func main() {
	// You may need a logger for debug purpose
	const (
		name = "serverLog.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	LOGF = log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	// Usage: LOGF.Println() or LOGF.Printf()

	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <port>", os.Args[0])
		return
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Port must be a number:", err)
		return
	}

	srv, err := startServer(port)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("Server listening on port", port)

	defer srv.lspServer.Close()

	for {
		c, pm, err := srv.lspServer.Read()
		if err != nil {
			if _, ok := srv.clientRequests[c]; ok {
				delete(srv.clientRequests, c)
				// client slice delete delayed to task assignation
			}

			if chunk, ok := srv.minerWorks[c]; ok {
				if chunk.clientID != -1 {
					crs, ok := srv.clientRequests[chunk.clientID]
					if ok {
						crs.waitList = append(crs.waitList, chunk.maxNonce)
					}
				}
				delete(srv.minerWorks, c)
				// miner slice delete delayed to task assignation
			}
		} else {
			var m bitcoin.Message
			if err := json.Unmarshal(pm, &m); err != nil {
				fmt.Println(err)
				continue
			}
			switch m.Type {
			case bitcoin.Join:
				srv.freeMiners = append(srv.freeMiners, c)
				srv.minerWorks[c] = minerWork{-1, 0}
				srv.assignTasks()
			case bitcoin.Request:
				msg := m.Data
				maxNonce := m.Upper
				srv.clients = append(srv.clients, c)
				srv.clientRequests[c] = newClientRequestStatus(msg, maxNonce)
				srv.assignTasks()
			case bitcoin.Result:
				hash := m.Hash
				nonce := m.Nonce
				cID := srv.minerWorks[c].clientID
				crs, ok := srv.clientRequests[cID]
				if !ok {
					continue
				}
				if hash < crs.minHash {
					crs.minHash = hash
					crs.minHashNonce = nonce
				}
				crs.finishedCnt++
				if crs.finishedCnt == crs.maxNonce/CHUNKSIZE+1 {
					res := bitcoin.NewResult(crs.minHash, crs.maxNonce)
					pres, err := json.Marshal(res)
					if err != nil {
						fmt.Println(err)
						continue
					}
					srv.lspServer.Write(cID, pres)
					srv.lspServer.CloseConn(cID)
					delete(srv.clientRequests, cID)
				}

				srv.minerWorks[c] = minerWork{-1, 0}
				srv.freeMiners = append(srv.freeMiners, c)
				srv.assignTasks()
			}
		}
	}
}

func (srv server) getNextChunk() (avail bool, chunk minerWork) {
	emptyWaitListTracker := make(map[int]struct{})
	for {
		if len(srv.clients) == 0 {
			return false, minerWork{-1, 0}
		}
		if len(srv.clients) == len(emptyWaitListTracker) {
			return false, minerWork{-1, 0}
		}
		cID := srv.clients[srv.cLoopIdx]
		cReq, ok := srv.clientRequests[cID]
		if !ok {
			if srv.cLoopIdx == len(srv.clients)-1 {
				srv.cLoopIdx = 0
			} else {
				srv.clients[srv.cLoopIdx] = srv.clients[len(srv.clients)-1]
			}
			srv.clients = srv.clients[:len(srv.clients)-1]
		} else {
			if len(cReq.waitList) != 0 {
				chunk = minerWork{cID, cReq.waitList[len(cReq.waitList)-1]}
				cReq.waitList = cReq.waitList[:len(cReq.waitList)-1]
				srv.cLoopIdx = (srv.cLoopIdx + 1) % len(srv.clients)
				return true, chunk
			} else {
				emptyWaitListTracker[cID] = struct{}{}
				srv.cLoopIdx = (srv.cLoopIdx + 1) % len(srv.clients)
			}
		}
	}
}

func (srv server) getNextMiner() (avail bool, miner int) {
	for {
		if len(srv.freeMiners) == 0 {
			return false, -1
		}
		miner = srv.freeMiners[len(srv.freeMiners)-1]
		srv.freeMiners = srv.freeMiners[:len(srv.freeMiners)-1]
		if _, ok := srv.minerWorks[miner]; ok {
			return true, miner
		}
	}

}

func (srv server) assignTasks() {
	for {
		chunkAvail, work := srv.getNextChunk()
		minerAvail, miner := srv.getNextMiner()
		if chunkAvail && minerAvail {
			l := work.maxNonce / CHUNKSIZE * CHUNKSIZE
			req := bitcoin.NewRequest(srv.clientRequests[work.clientID].msg, l, work.maxNonce)
			pReq, err := json.Marshal(req)
			if err != nil {
				fmt.Println(err)
				return
			}
			srv.lspServer.Write(miner, pReq)
		} else {
			return
		}
	}
}
