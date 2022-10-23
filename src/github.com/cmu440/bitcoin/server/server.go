package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
)

const CHUNKSIZE uint64 = 10000

type server struct {
	lspServer lsp.Server
	clients   clientList
	miners    minerList
}

func startServer(port int) (*server, error) {
	lspServer, err := lsp.NewServer(port, lsp.NewParams())
	return &server{
		lspServer: lspServer,
		miners:    newMinerList(),
		clients:   newClientList(),
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
			if srv.clients.check(c) {
				LOGF.Printf("client %d disconnected\n", c)
				srv.clients.delete(c)
			}

			if srv.miners.check(c) {
				LOGF.Printf("miner %d disconnected\n", c)
				work := srv.miners.delete(c)
				srv.clients.addBackWork(work)
			}
		} else {
			var m bitcoin.Message
			if err := json.Unmarshal(pm, &m); err != nil {
				fmt.Println(err)
				continue
			}
			LOGF.Println(m.String() + " from " + strconv.Itoa(c))
			switch m.Type {
			case bitcoin.Join:
				srv.miners.add(c)
			case bitcoin.Request:
				srv.clients.add(c, m.Data, m.Upper)
			case bitcoin.Result:
				cID := srv.miners.getWorkingClient(c)
				if cID == -1 {
					LOGF.Println("Miner ", c, " not working!")
					continue
				}
				finished, minHash, minHashNonce := srv.clients.recvResult(m.Hash, m.Nonce, cID)
				if finished {
					res := bitcoin.NewResult(minHash, minHashNonce)
					pres, err := json.Marshal(res)
					if err != nil {
						fmt.Println(err)
						continue
					}
					LOGF.Printf("Trying to write to client %d", cID)
					if err := srv.lspServer.Write(cID, pres); err != nil {
						fmt.Println(err)
						continue
					}
				}
				srv.miners.freeWork(c)
			}
		}
		srv.assignTasks()
	}
}

func (srv *server) assignTasks() {
	for {
		if srv.miners.checkNext() && srv.clients.checkNext() {
			miner := srv.miners.getNext()
			msg, work := srv.clients.getNext()
			srv.miners.assignWork(miner, work)
			minNonce := work.maxNonce / CHUNKSIZE * CHUNKSIZE
			req := bitcoin.NewRequest(msg, minNonce, work.maxNonce)
			pReq, err := json.Marshal(req)
			if err != nil {
				LOGF.Println(err)
				return
			}
			LOGF.Printf("Trying to write to miner %d\n", miner)
			if err = srv.lspServer.Write(miner, pReq); err != nil {
				LOGF.Println("Error during write: " + err.Error())
				continue
			}
		} else {
			return
		}
	}
}
