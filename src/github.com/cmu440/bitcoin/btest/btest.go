package main

import (
	"bytes"
	"fmt"
	"log"
	"os/exec"
	// "strings"
)

const (
	serverArg0 = "/Users/kevin-m1/go/bin/server"
	minerArg0 = "/Users/kevin-m1/go/bin/miner"
	clientArg0 = "/Users/kevin-m1/go/bin/client"
	addr = "localhost:6060"
	port = "6060"
	clientCount = 8
	minerCount = 4
	maxNonce = "9999"
)

var (
	schan = make(chan error)
	cchan = make(chan struct{})
	mchan = make(chan struct{})
)

func main() {
	go runServer()
	go runClient()
	go runMiner()

	for {
		select {
		case err := <-schan:
			log.Printf("Server exited with error: %v", err)
		case <-cchan:
			log.Printf("All client exited")
		case <-mchan:
			log.Printf("All miner exited")
		}
	}
}

func runServer() {
	serverCmd := *exec.Command(serverArg0, port)
	var serverOut bytes.Buffer
	serverCmd.Stdout = &serverOut
	err := serverCmd.Start()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Starting server with: %v", serverCmd.Args)
	err = serverCmd.Wait()
	schan <- err
}

func runClient() {
	clientCmd := make([]exec.Cmd, clientCount)
	clientOut := make([]bytes.Buffer, clientCount)

	allMessages := []string{"zero", "one", "two", "three", "four", "five", "six", "seven"}

	for i := 0; i < clientCount; i++ {
		clientCmd[i] = *exec.Command(clientArg0, addr, allMessages[i], maxNonce)
		clientCmd[i].Stdout = &clientOut[i]
		err := clientCmd[i].Start()
		log.Printf("Starting client %d with: %v", i, clientCmd[i].Args)
		if err != nil {
			log.Fatal(err)
		}
	}

	for i := 0; i < clientCount; i++ {
		err := clientCmd[i].Wait()
		log.Printf("Client %d (spawn order) returned with error: %v", i, err)
		log.Printf("Client %d output: %q", i, clientOut[i].String())
	}

	cchan <- struct{}{}
}


func runMiner() {
	minerCmd := make([]exec.Cmd, minerCount)
	minerOut := make([]bytes.Buffer, minerCount)


	for i := 0; i < minerCount; i++ {
		minerCmd[i] = *exec.Command(minerArg0, addr)
		minerCmd[i].Stdout = &minerOut[i]
		err := minerCmd[i].Start()
		log.Printf("Starting miner %d with: %v", i, minerCmd[i].Args)
		if err != nil {
			log.Fatal(err)
		}
	}

	err := minerCmd[1].Process.Kill()
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < minerCount; i++ {
		err := minerCmd[i].Wait()
		log.Printf("Miner %d (spawn order) returned with error: %v", i, err)
		fmt.Printf("Miner %d output: %q", i, minerOut[i].String())
	}

	mchan <- struct{}{}
}
