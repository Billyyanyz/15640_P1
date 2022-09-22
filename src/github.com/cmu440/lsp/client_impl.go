// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/cmu440/lspnet"
)

type client struct {
	connID  int
	readSeqNum int
	writeSeqNum int
	udpConn *lspnet.UDPConn
	// Signals
	stopMainRoutine   chan struct{}
	stopReadRoutine   chan struct{}
	connectionSuccess chan struct{}
	startReading      chan struct{} // User wakes up read routine

	readMessage chan MessageError
	readPayload chan PayloadError
}

type PayloadError struct {
	payload []byte
	err     error
}
type MessageError struct {
	message Message
	err     error
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// initialSeqNum is an int representing the Initial Sequence Number (ISN) this
// client must use. You may assume that sequence numbers do not wrap around.
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, initialSeqNum int, params *Params) (Client, error) {
	addr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	conn, err := lspnet.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, err
	}

	c := &client{
		connID:            0,
		readSeqNum:        0,
		writeSeqNum:       0,
		udpConn:           conn,
		stopMainRoutine:   make(chan struct{}),
		stopReadRoutine:   make(chan struct{}),
		connectionSuccess: make(chan struct{}),
		startReading:      make(chan struct{}),
		readMessage:       make(chan MessageError),
		readPayload:       make(chan PayloadError),
	}

	go c.MainRoutine()
	go c.ReadRoutine()

	connectMsg := NewConnect(c.writeSeqNum)
	connectRawMsg, err := json.Marshal(connectMsg)
	if err != nil {
		return nil, err
	}
	_, err = c.udpConn.Write(connectRawMsg)
	
	<-c.connectionSuccess

	return c, nil
}

func (c *client) MainRoutine() {
	for {
		select {
		case <-c.stopMainRoutine:
			return
		case me := <-c.readMessage:
			message := me.message
			err := me.err
			fmt.Printf("%s\n", &message)
			if err != nil {
				c.readPayload <- PayloadError{
					message.Payload,
					err,
				}
			}
			switch message.Type {
			case MsgConnect:
				c.connID = message.ConnID
				c.connectionSuccess <- struct{}{}
				// Connection establishment Ack goes through
				// public read API
				c.readPayload <- PayloadError{
					message.Payload,
					nil,
				}
			}
		}
	}
}

func (c *client) ReadRoutine() {
	for {
		select {
		case <-c.stopReadRoutine:
			return
		default:
			rawMsg := make([]byte, 2048)
			var me MessageError
			_, err := c.udpConn.Read(rawMsg)
			if err != nil {
				me.err = err
			}
			json.Unmarshal(rawMsg, me.message)
			c.readMessage <- me
		}
	}
}
func (c *client) ConnID() int {
	return c.connID
}

func (c *client) Read() ([]byte, error) {
	c.startReading <- struct{}{}
	pe := <-c.readPayload
	return pe.payload, pe.err
}

func (c *client) Write(payload []byte) error {
	return errors.New("not yet implemented")
}

func (c *client) Close() error {
	c.stopReadRoutine <- struct{}{}
	c.stopMainRoutine <- struct{}{}
	c.udpConn.Close()
	return errors.New("not yet implemented")
}
