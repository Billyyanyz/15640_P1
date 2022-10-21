package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"log"
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/cmu440/lsp"
)

// Attempt to connect miner as a client to the server.
func joinWithServer(hostport string) (lsp.Client, error) {
	// You will need this for randomized isn
	seed := rand.NewSource(time.Now().UnixNano())
	isn := rand.New(seed).Intn(int(math.Pow(2, 8)))

	c, err := lsp.NewClient(hostport, isn, lsp.NewParams())
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	j := bitcoin.NewJoin()
	pj, err := json.Marshal(j)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	if err := c.Write(pj); err != nil {
		fmt.Println(err)
		return nil, err
	}

	return c, nil
}

var LOGF *log.Logger

func main() {
	// You may need a logger for debug purpose
	const (
		name = "minerLog.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport>", os.Args[0])
		return
	}

	hostport := os.Args[1]
	miner, err := joinWithServer(hostport)
	if err != nil {
		fmt.Println("Failed to join with server:", err)
		return
	}

	defer miner.Close()

	for {
		pm, err := miner.Read()
		if err != nil {
			return
		} else {
			var m bitcoin.Message
			if err := json.Unmarshal(pm, &m); err != nil {
				fmt.Println(err)
				continue
			}
			switch m.Type {
			case bitcoin.Request:
				minHash := uint64(math.MaxUint64)
				minHashNonce := uint64(0)
				for i := m.Lower; i <= m.Upper; i++ {
					h := bitcoin.Hash(m.Data, i)
					if h < minHash {
						minHash = h
						minHashNonce = i
					}
				}

				res := bitcoin.NewResult(minHash, minHashNonce)
				pres, err := json.Marshal(res)
				if err != nil {
					fmt.Println(err)
					continue
				}
				if err := miner.Write(pres); err != nil {
					fmt.Println(err)
					continue
				}
			default:
				fmt.Println("Miner received invalid message!")
			}
		}
	}

}
