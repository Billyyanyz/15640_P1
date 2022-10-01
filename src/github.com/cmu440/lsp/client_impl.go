// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"strconv"
	"time"

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
	await       bool

	// Cache
	sw               oldSlidingWindowSender
	receivedMessages map[int]MessageError
	writeBuffer      map[int]Message

	// Signals
	stopMainRoutine   chan struct{}
	stopReadRoutine   chan struct{}
	connectionSuccess chan struct{}

	readFunctionCall     chan struct{}
	readMessageGeneral   chan MessageError
	readFunctionCallRes  chan *PayloadError
	writeAck             chan Message // Wake up WriteRoutine to Ack server's packet
	writeFunctionCall    chan []byte
	writeFunctionCallRes chan error
	handleServerAck      chan Message
	handleServerAckRes   chan struct{}
	handleServerCAck     chan Message
	handleServerCAckRes  chan struct{}

	// epoch events
	epochCnt       int
	epochTimer     *time.Ticker
	epochSinceLast int
	// Only WriteRoutine can touch the following
	sentState     bool // Any message sent last epoch?
	sentStateChan chan bool
	getSentState  chan struct{}
	setSentState  chan bool
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
		connID:      0,
		readSeqNum:  initialSeqNum,
		writeSeqNum: initialSeqNum,
		state:       CSInit,
		udpConn:     conn,
		params:      params,
		await:       false,

		receivedMessages: make(map[int]MessageError),
		writeBuffer:      make(map[int]Message),

		stopMainRoutine:   make(chan struct{}),
		stopReadRoutine:   make(chan struct{}),
		connectionSuccess: make(chan struct{}),

		readFunctionCall:     make(chan struct{}),
		readMessageGeneral:   make(chan MessageError),
		readFunctionCallRes:  make(chan *PayloadError),
		writeAck:             make(chan Message),
		writeFunctionCall:    make(chan []byte, 1),
		writeFunctionCallRes: make(chan error),
		handleServerAck:      make(chan Message),
		handleServerAckRes:   make(chan struct{}),
		handleServerCAck:     make(chan Message),
		handleServerCAckRes:  make(chan struct{}),

		epochCnt: 0,
		epochTimer: time.NewTicker(time.Millisecond *
			time.Duration(params.EpochMillis)),
		epochSinceLast: 0,
		sentState:      false,
		sentStateChan:  make(chan bool),
		getSentState:   make(chan struct{}),
		setSentState:   make(chan bool),
	}

	go c.MainRoutine()
	go c.ReadRoutine()

	connectMsg := NewConnect(c.writeSeqNum)
	connectRawMsg, err := json.Marshal(connectMsg)
	if err != nil {
		return nil, err
	}
	_, err = c.udpConn.Write(connectRawMsg)
	if err != nil {
		return nil, err
	}

	// Block until we get the first Ack or timeout
	handShakeTicker := time.NewTicker(time.Millisecond *
		time.Duration(params.EpochMillis))
	handShakeEpochCnt := 0
	successFlag := false
	defer handShakeTicker.Stop()
	for handShakeEpochCnt <= params.EpochLimit {
		select {
		case <-c.connectionSuccess:
			successFlag = true
			break
		case <-handShakeTicker.C:
			_, err = c.udpConn.Write(connectRawMsg)
			if err != nil {
				return nil, err
				// TODO: Deal with closing connection here
			}
			handShakeEpochCnt++
		}
		if successFlag {
			break
		}
	}
	if handShakeEpochCnt > params.EpochLimit {
		// Connection timeout
		return nil, errors.New("Connection handshake timeout")
		// TODO: Deal with closing connection here
	}

	sw := newOldSlidingWindowSender(initialSeqNum,
		params.WindowSize,
		params.MaxUnackedMessages)
	c.sw = sw

	go c.WriteRoutine()
	return c, nil
}

func (c *client) MainRoutine() {
	for {
		select {
		case <-c.stopMainRoutine:
			return
		case me := <-c.readMessageGeneral:
			c.processReceivedMsg(me)
		case <-c.readFunctionCall:
			me, found := c.receivedMessages[c.readSeqNum+1]
			if !found {
				c.await = true
			} else {
				delete(c.receivedMessages, c.readSeqNum+1)
				c.readSeqNum++
				c.readFunctionCallRes <- &PayloadError{
					me.message.Payload,
					me.err,
				}
			}
		case <-c.epochTimer.C:
			timeout := c.clientEpochTick()
			if timeout {
				return
				// TODO: Deal with closing
			}
		}
	}
}

// Process a message received from client ReadRoutine
// Can only be called from client MainRoutine
func (c *client) processReceivedMsg(me MessageError) {
	message := me.message
	err := me.err
	clientImplLog("Reading message: " + message.String())
	if err != nil {
		clientImplLog("Error: " + err.Error())
	}
	c.epochSinceLast = 0
	switch message.Type {
	case MsgConnect:
		clientImplLog("--PANIC-- Client receives connect message!")
		return
	case MsgData:
		clientImplLog("Reading data message: " + message.String())
		if !c.ensureDataValidity(&message) {
			clientImplLog("Corrupted data message, discarding...: " +
				message.String())
				// TODO: Close
			return
		}
		c.receivedMessages[message.SeqNum] = me
		me, found := c.receivedMessages[c.readSeqNum+1]
		if c.await && found {
			delete(c.receivedMessages, c.readSeqNum+1)
			c.readSeqNum++
			c.readFunctionCallRes <- &PayloadError{
				me.message.Payload,
				me.err,
			}
		}
		c.writeAck <- message
	case MsgAck:
		clientImplLog("Reading Ack message: " + message.String())
		if c.state == CSInit {
			c.connID = message.ConnID
			c.state = CSConnected
			c.connectionSuccess <- struct{}{}
			return
		}
		if message.SeqNum == 0 {
			// Server heartbeat, no need to change SW
			clientImplLog("Server heartbeat: " + message.String())
			return
		}
		c.handleServerAck <- message
		<-c.handleServerAckRes
	case MsgCAck:
		clientImplLog("Reading CAck message: " + message.String())
		if c.state == CSInit {
			c.connID = message.ConnID
			c.state = CSConnected
			c.connectionSuccess <- struct{}{}
			return
		}
		c.handleServerCAck <- message
		<-c.handleServerCAckRes
	}
}

// Epoch tick hitting our client, return whether the server has timeout
// Can only be called from client MainRoutine
func (c *client) clientEpochTick() bool {
	clientImplLog("Client epoch " + strconv.Itoa(c.epochCnt))
	c.epochCnt++
	c.epochSinceLast++
	if c.epochSinceLast > c.params.EpochLimit {
		return true
	}
	clientImplLog("Remaining epoches to waste: " +
		strconv.Itoa(c.params.EpochLimit-c.epochSinceLast))
	c.getSentState <- struct{}{}
	s := <-c.sentStateChan
	if !s {
		// Heartbeat
		c.writeAck <- *NewAck(c.connID, 0)
	}
	c.setSentState <- false
	return false
}

func (c *client) ensureDataValidity(m *Message) bool {
	if len(m.Payload) > m.Size {
		return false
	} else if len(m.Payload) < m.Size {
		m.Payload = m.Payload[:m.Size]
	}
	if CalculateChecksum(m.ConnID, m.SeqNum, m.Size, m.Payload) != m.Checksum {
		return false
	}
	return true
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
			seqNum := c.sw.getSeqNum()
			writeSize := len(payload)
			checkSum := CalculateChecksum(c.connID,
				seqNum,
				writeSize,
				payload)
			writeMsg := NewData(c.connID,
				seqNum,
				writeSize,
				payload,
				checkSum)
			clientImplLog("Backing up message: " + string(writeMsg.String()))
			c.sw.backupUnsentMsg(writeMsg)
			c.writeFunctionCallRes <- nil
			c.sendMessagefromSW()
			// TODO: What to return when there's an error?
		case message := <-c.writeAck:
			writeMsg := NewAck(message.ConnID, message.SeqNum)
			if message.SeqNum != 0 {
				clientImplLog("Ack'ing to server: " +
					string(writeMsg.String()))
			} else {
				clientImplLog("Client heartbeat: " +
					string(writeMsg.String()))
			}
			b, err := json.Marshal(writeMsg)
			if err != nil {
				clientImplLog("Error when Ack'ing to server: " +
					err.Error())
				//TODO: DONT RETURN
				return
			}
			_, err = c.udpConn.Write(b)
			if err != nil {
				clientImplLog("Error when Ack'ing to server: " +
					err.Error())
				return
			}
			c.sentState = true
		case message := <-c.handleServerAck:
			c.sw.ackMessage(message.SeqNum)
			c.sendMessagefromSW()
			c.handleServerAckRes <- struct{}{}
		case message := <-c.handleServerCAck:
			c.sw.cackMessage(message.SeqNum)
			c.sendMessagefromSW()
			c.handleServerCAckRes <- struct{}{}
		case <-c.getSentState:
			c.sentStateChan <- c.sentState
		case s := <-c.setSentState:
			c.sentState = s
		}
	}
}

func (c *client) sendMessagefromSW() {
	_, writeMsg := c.sw.nextMsgToSend()
	if writeMsg == nil {
		return
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
	c.sw.markMessageSent(writeMsg)
	c.sentState = true
}

func (c *client) ConnID() int {
	return c.connID
}

func (c *client) Read() ([]byte, error) {
	c.readFunctionCall <- struct{}{}
	pe := <-c.readFunctionCallRes
	return pe.payload, pe.err
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
