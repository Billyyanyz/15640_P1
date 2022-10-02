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
	CSClosing
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
	sw               slidingWindowSender
	receivedMessages map[int]MessageError
	writeBuffer      map[int]Message

	// Signals
	stopMainRoutine   chan struct{}
	stopReadRoutine   chan struct{}
	stopWriteRoutine  chan struct{}
	connectionSuccess chan struct{}

	readFunctionCall     chan struct{}
	readMessageGeneral   chan MessageError
	readFunctionCallRes  chan *PayloadError
	writeAck             chan Message
	writeFunctionCall    chan []byte
	writeFunctionCallRes chan error
	handleServerAck      chan MessageEpoch
	handleServerAckRes   chan struct{}
	handleServerCAck     chan MessageEpoch
	handleServerCAckRes  chan struct{}

	// epoch events
	epochCnt       int
	epochTimer     *time.Ticker
	epochSinceLast int
	getEpochCnt    chan struct{} // write -> main
	epochCntChan   chan int      // main -> write
	resend         chan int      // main -> write
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

type PayloadEpoch struct {
	payload []byte
	epoch   int
}

type MessageEpoch struct {
	message Message
	epoch   int
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
		stopWriteRoutine:  make(chan struct{}),
		connectionSuccess: make(chan struct{}),

		readFunctionCall:     make(chan struct{}),
		readMessageGeneral:   make(chan MessageError),
		readFunctionCallRes:  make(chan *PayloadError),
		writeAck:             make(chan Message),
		writeFunctionCall:    make(chan []byte, 1),
		writeFunctionCallRes: make(chan error),
		handleServerAck:      make(chan MessageEpoch),
		handleServerAckRes:   make(chan struct{}),
		handleServerCAck:     make(chan MessageEpoch),
		handleServerCAckRes:  make(chan struct{}),

		epochCnt: 0,
		epochTimer: time.NewTicker(time.Millisecond *
			time.Duration(params.EpochMillis)),
		epochSinceLast: 0,
		getEpochCnt:    make(chan struct{}),
		epochCntChan:   make(chan int),
		resend:         make(chan int),
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
				c.udpConn.Close()
				return nil, err
			}
			handShakeEpochCnt++
		}
		if successFlag {
			break
		}
	}
	if !successFlag {
		// Connection timeout
		c.udpConn.Close()
		return nil, errors.New("Connection handshake timeout")
	}

	sw := newSlidingWindowSender(initialSeqNum,
		params.WindowSize,
		params.MaxUnackedMessages,
		params.MaxBackOffInterval)
	c.sw = sw

	go c.WriteRoutine()
	return c, nil
}

func (c *client) MainRoutine() {
	for {
		select {
		case <-c.stopMainRoutine:
			if c.state == CSClosing {
				return
			} else {
				c.state = CSClosing
			}
		case me := <-c.readMessageGeneral:
			ret := c.processReceivedMsg(me)
			if ret < 0 {
				c.udpConn.Close()
				return
			}
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
				c.udpConn.Close()
				return
			}
		case <-c.getEpochCnt:
			c.epochCntChan <- c.epochCnt
		}
	}
}

// Process a message received from client ReadRoutine
// Return 0 for success, negative for failure
// Can only be called from client MainRoutine
func (c *client) processReceivedMsg(me MessageError) int {
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
		return -1
	case MsgData:
		clientImplLog("Reading data message: " + message.String())
		if !c.ensureDataValidity(&message) {
			clientImplLog("Corrupted data message, discarding...: " +
				message.String())
			// Deal as if it succeeded
			return 0
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
			c.await = false
		}
		c.writeAck <- message
	case MsgAck:
		clientImplLog("Reading Ack message: " + message.String())
		if c.state == CSInit {
			c.connID = message.ConnID
			c.state = CSConnected
			c.connectionSuccess <- struct{}{}
			return 0
		}
		if message.SeqNum == 0 {
			// Server heartbeat, no need to change SW
			clientImplLog("Server heartbeat: " + message.String())
			return 0
		}
		c.handleServerAck <- MessageEpoch{message, c.epochCnt}
		<-c.handleServerAckRes
	case MsgCAck:
		clientImplLog("Reading CAck message: " + message.String())
		if c.state == CSInit {
			c.connID = message.ConnID
			c.state = CSConnected
			c.connectionSuccess <- struct{}{}
			return 0
		}
		c.handleServerCAck <- MessageEpoch{message, c.epochCnt}
		<-c.handleServerCAckRes
	}
	return 0
}

// Epoch tick hitting our client, return whether the server has timeout
// Can only be called from client MainRoutine
func (c *client) clientEpochTick() bool {
	clientImplLog("Client epoch " + strconv.Itoa(c.epochCnt))
	c.epochCnt++
	c.epochSinceLast++
	// Detect timeout and send heartbeat if needed
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
	// Resend all Unacked messages
	//c.resend <- c.epochCnt
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
			// We are sure udpConn is closed
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
		case <-c.stopWriteRoutine:
			return
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
			clientImplLog("Backing up message: " +
				string(writeMsg.String()))
			c.sw.backupUnsentMsg(writeMsg)
			// We should only get epoch count from MainRoutine if
			// the WriteRoutine code is not generated from
			// MainRoutine. Otherwise we will deadlock. This
			// probably means WriteRoutine is utterly useless.
			clientImplLog("Get Epoch count" + writeMsg.String())
			//c.getEpochCnt <- struct{}{}
			clientImplLog("Received Epoch count" + writeMsg.String())
			//epoch := <-c.epochCntChan
			//err := c.sendMessagefromSW(epoch)
			err := c.sendMessagefromSW(10000)
			c.writeFunctionCallRes <- err
		case message := <-c.writeAck:
			writeMsg := NewAck(message.ConnID, message.SeqNum)
			err := c.sendMessage(writeMsg)
			if err != nil {
				return
			}
		case mepoch := <-c.handleServerAck:
			message := mepoch.message
			epoch := mepoch.epoch
			c.sw.ackMessage(message.SeqNum)
			err := c.sendMessagefromSW(epoch)
			c.handleServerAckRes <- struct{}{}
			if err != nil {
				return
			}
		case mepoch := <-c.handleServerCAck:
			message := mepoch.message
			epoch := mepoch.epoch
			c.sw.cackMessage(message.SeqNum)
			err := c.sendMessagefromSW(epoch)
			c.handleServerCAckRes <- struct{}{}
			if err != nil {
				return
			}
		case epoch := <-c.resend:
			resendList := c.sw.resendMessageList(epoch)
			for _, m := range resendList {
				err := c.sendMessage(m)
				if err != nil {
					return
				}
			}
		case <-c.getSentState:
			c.sentStateChan <- c.sentState
		case s := <-c.setSentState:
			c.sentState = s
		}
	}
}

func (c *client) sendMessagefromSW(epoch int) error {
	_, writeMsg := c.sw.nextMsgToSend()
	if writeMsg == nil {
		return nil
	}
	clientImplLog("Writing message: " + string(writeMsg.String()))
	b, err := json.Marshal(writeMsg)
	var ret error = nil
	if err != nil {
		clientImplLog("Error writing message: " +
			string(writeMsg.String()))
		ret = err
	}
	_, err = c.udpConn.Write(b)
	if err != nil {
		clientImplLog("Error writing message: " +
			string(writeMsg.String()))
		ret = err
	}
	clientImplLog("before fetching epoch cnt")
	c.sw.markNextMessageSent(writeMsg, epoch)
	c.sentState = true
	return ret
}

func (c *client) sendMessage(writeMsg *Message) error {
	b, err := json.Marshal(writeMsg)
	var ret error = nil
	if err != nil {
		clientImplLog("Error when sending to server: " + err.Error())
		ret = err
	}
	_, err = c.udpConn.Write(b)
	if err != nil {
		clientImplLog("Error when sending to server: " + err.Error())
		ret = err
	}
	c.sentState = true
	return ret
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
	c.stopWriteRoutine <- struct{}{}
	return c.udpConn.Close()
}
