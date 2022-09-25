// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
)

type ClientState int

const (
	CSInit ClientState = iota
	CSConnected
)

type client struct {
	// States
	connID      int
	readSeqNum  int
	writeSeqNum int
	state       ClientState
	udpConn     *lspnet.UDPConn
	// Cache
	receivedMessages map[int]MessageError
	// Signals
	stopMainRoutine   chan struct{}
	stopReadRoutine   chan struct{}
	connectionSuccess chan struct{}

	readFunctionCall  chan struct{}
	readMessageGeneral          chan MessageError
	readFunctionCallRes          chan *PayloadError
	writeAck          chan Message  // We wake up write routine to Ack server's packet
	writeFunctionCall chan []byte   // User wakes up write routine
	writeFunctionCallRes chan error
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
		connID:               0,
		readSeqNum:           0,
		writeSeqNum:          0,
		state:                CSInit,
		udpConn:              conn,
		receivedMessages: 	      make(map[int]MessageError),
		stopMainRoutine:      make(chan struct{}),
		stopReadRoutine:      make(chan struct{}),
		connectionSuccess:    make(chan struct{}),
		readFunctionCall:     make(chan struct{}),
		writeFunctionCall:    make(chan []byte),
		readMessageGeneral:          make(chan MessageError),
		writeAck:             make(chan Message),
		readFunctionCallRes:          make(chan *PayloadError),
		writeFunctionCallRes: make(chan error),
	}

	go c.MainRoutine()
	go c.ReadRoutine()
	go c.WriteRoutine()

	connectMsg := NewConnect(c.writeSeqNum)
	connectRawMsg, err := json.Marshal(connectMsg)
	if err != nil {
		return nil, err
	}
	_, err = c.udpConn.Write(connectRawMsg)
	if err != nil {
		return nil, err
	}
	// Block until we get the first Ack
	<-c.connectionSuccess
	return c, nil
}

func (c *client) MainRoutine() {
	for {
		select {
		case <-c.stopMainRoutine:
			return
		case me := <-c.readMessageGeneral:
			message := me.message
			err := me.err
			clientImplLog("Reading message: " + message.String())
			if err != nil {
				clientImplLog("Error: " + err.Error())
			}
			switch message.Type {
			case MsgConnect:
				clientImplLog("--PANIC-- Client receives connect message!")
				return
			case MsgData:
				clientImplLog("Reading data message: " + string(message.Payload))
				c.receivedMessages[message.SeqNum] = me
				c.writeAck <- message
			case MsgAck:
				clientImplLog("Reading Ack message: " + string(message.Payload))
				// TODO: Implement sliding windows
				if c.state == CSInit {
					c.connID = message.ConnID
					c.state = CSConnected
					c.connectionSuccess <- struct{}{}
				}
			}
		case <-c.readFunctionCall:
			me, found := c.receivedMessages[c.readSeqNum + 1]
			if !found {
				c.readFunctionCallRes <- nil
			} else {
				delete(c.receivedMessages, c.readSeqNum + 1)
				c.readSeqNum++
				c.readFunctionCallRes <- &PayloadError{
					me.message.Payload,
					me.err,
				}
			}
		}
	}
}

func (c *client) ProcessReceivedMessage() *MessageError {
	for sn, me := range c.receivedMessages {
		if sn == c.readSeqNum + 1 {
			return &me
		}
	}
	return nil
}

func (c *client) ReadRoutine() {
	for {
		select {
		case <-c.stopReadRoutine:
			return
		default:
			rawMsg := make([]byte, 2048)
			var me MessageError
			n, _, err := c.udpConn.ReadFromUDP(rawMsg)
			if err != nil {
				me.err = err
			}
			err = json.Unmarshal(rawMsg[:n], &me.message)
			if err != nil {
				me.err = err
			}
			c.readMessageGeneral <- me
		}
	}
}

func (c *client) WriteRoutine() {
	for {
		select {
		case payload := <-c.writeFunctionCall:
			c.writeSeqNum++
			seqNum := c.writeSeqNum // TODO: change to use sliding window
			writeSize := len(payload)
			checkSum := CalculateChecksum(c.connID, seqNum, writeSize, payload)
			writeMsg := NewData(c.connID, seqNum, writeSize, payload, checkSum)
			clientImplLog("Writing message: " + string(writeMsg.String()))
			b, err := json.Marshal(writeMsg)
			if err != nil {
				c.writeFunctionCallRes <- err
			}
			_, err = c.udpConn.Write(b)
			c.writeFunctionCallRes <- err
		case message := <-c.writeAck:
			writeMsg := NewAck(message.ConnID, message.SeqNum)
			clientImplLog("Ack'ing to server: " + string(writeMsg.String()))
			b, err := json.Marshal(writeMsg)
			if err != nil {
				clientImplLog("Error when Ack'ing to server: " + err.Error())
				return
			}
			_, err = c.udpConn.Write(b)
			if err != nil {
				clientImplLog("Error when Ack'ing to server: " + err.Error())
				return
			}
		}
	}
}

func (c *client) ConnID() int {
	return c.connID
}

func (c *client) Read() ([]byte, error) {
	for {
		c.readFunctionCall <- struct{}{}
		pe := <-c.readFunctionCallRes
		if pe != nil {
			return pe.payload, pe.err
		}
	}
}

func (c *client) Write(payload []byte) error {
	c.writeFunctionCall <- payload
	return <-c.writeFunctionCallRes
}

func (c *client) Close() error {
	c.stopReadRoutine <- struct{}{}
	c.stopMainRoutine <- struct{}{}
	c.udpConn.Close()
	return errors.New("not yet implemented")
}
