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
	params      *Params
	// Cache
	sw               slidingWindowSender2
	receivedMessages map[int]MessageError
	writeBuffer	 map[int]Message
	// Signals
	stopMainRoutine   chan struct{}
	stopReadRoutine   chan struct{}
	connectionSuccess chan struct{}

	readFunctionCall     chan struct{}
	readMessageGeneral   chan MessageError
	readFunctionCallRes  chan *PayloadError
	writeAck             chan Message // We wake up write routine to Ack server's packet
	writeFunctionCall    chan []byte  // User wakes up write routine
	writeFunctionCallRes chan error
	handleServerAck chan Message
	handleServerAckRes chan struct{}
	handleServerCAck chan Message
	handleServerCAckRes chan struct{}
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

	// slidingWindow := newSlidingWindowSender(0, params.WindowSize, params.MaxUnackedMessages)
	c := &client{
		connID:               0,
		readSeqNum:           0,
		writeSeqNum:          0,
		state:                CSInit,
		udpConn:              conn,
		params:               params,

		// sw:                   slidingWindow,
		receivedMessages:     make(map[int]MessageError),
		writeBuffer:	      make(map[int]Message),

		stopMainRoutine:      make(chan struct{}),
		stopReadRoutine:      make(chan struct{}),
		connectionSuccess:    make(chan struct{}),

		readFunctionCall:     make(chan struct{}),
		readMessageGeneral:   make(chan MessageError),
		readFunctionCallRes:  make(chan *PayloadError),
		writeAck:             make(chan Message),
		writeFunctionCall:    make(chan []byte),
		writeFunctionCallRes: make(chan error),
		handleServerAck: make(chan Message),
		handleServerAckRes: make(chan struct{}),
		handleServerCAck: make(chan Message),
		handleServerCAckRes: make(chan struct{}),
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
	sw := NewSlidingWindowSender2(params.WindowSize, params.MaxUnackedMessages)
	c.sw = sw
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
				if c.state == CSInit {
					c.connID = message.ConnID
					c.state = CSConnected
					c.connectionSuccess <- struct{}{}
					continue
				}
				c.handleServerAck <- message
				<-c.handleServerAckRes
			case MsgCAck:
				clientImplLog("Reading CAck message: " + string(message.Payload))
				if c.state == CSInit {
					c.connID = message.ConnID
					c.state = CSConnected
					c.connectionSuccess <- struct{}{}
					continue
				}
				c.handleServerCAck <- message
				<-c.handleServerCAckRes
			}
		case <-c.readFunctionCall:
			me, found := c.receivedMessages[c.readSeqNum+1]
			if !found {
				c.readFunctionCallRes <- nil
			} else {
				delete(c.receivedMessages, c.readSeqNum+1)
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
		if sn == c.readSeqNum+1 {
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
			seqNum := c.sw.GetSeqNum()
			writeSize := len(payload)
			checkSum := CalculateChecksum(c.connID, seqNum, writeSize, payload)
			writeMsg := NewData(c.connID, seqNum, writeSize, payload, checkSum)
			clientImplLog("Backing up message: " + string(writeMsg.String()))
			c.sw.BackupUnsentMsg(writeMsg)
			c.writeFunctionCallRes <- nil
			// TODO: What to return when there's an error?
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
		case message := <-c.handleServerAck:
			c.sw.AckMessage(message.SeqNum)
			c.handleServerAckRes <- struct{}{}
		case message := <-c.handleServerCAck:
			c.sw.CAckMessage(message.SeqNum)
			c.handleServerCAckRes <- struct{}{}
		default:
			_, writeMsg := c.sw.NextMsgToSend()
			if writeMsg == nil {
				continue
			}
			clientImplLog("Writing message: " + string(writeMsg.String()))
			b, err := json.Marshal(writeMsg)
			if err != nil {
				clientImplLog("Error writing message: " +
				string(writeMsg.String()))
			}
			_, err = c.udpConn.Write(b)
			if err != nil {
				clientImplLog("Error writing message: " +
				string(writeMsg.String()))
			}
			// TODO: Handle the error here
			c.sw.MarkMessageSent(writeMsg)
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
