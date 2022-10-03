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
	CSClosed
)

const (
	EFATAL = 1
	EAGAIN = 2
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
	connectionSuccess chan struct{}
	pauseReading      chan struct{}
	restartReading    chan struct{}

	readMessageGeneral   chan MessageError
	readFunctionCallRes  chan *PayloadError
	writeFunctionCall    chan []byte
	writeFunctionCallRes chan error

	// epoch events
	epochCnt       int
	epochTimer     *time.Ticker
	epochSinceLast int
	sentState      bool // Any message sent last epoch?

	// For closing operation
	closeFunctionCall    chan struct{}
	closeFunctionCallRes chan struct{}
	readFunctionAlive    bool
	notifyCloseCaller    bool
}

type PayloadError struct {
	payload []byte
	err     error
}

type MessageError struct {
	message *Message
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
		pauseReading:      make(chan struct{}),
		restartReading:    make(chan struct{}),

		readMessageGeneral:   make(chan MessageError),
		readFunctionCallRes:  make(chan *PayloadError),
		writeFunctionCall:    make(chan []byte, 1),
		writeFunctionCallRes: make(chan error),

		epochCnt: 0,
		epochTimer: time.NewTicker(time.Millisecond *
			time.Duration(params.EpochMillis)),
		epochSinceLast: 0,
		sentState:      false,

		closeFunctionCall:    make(chan struct{}),
		closeFunctionCallRes: make(chan struct{}),
		readFunctionAlive:    true,
		notifyCloseCaller:    false,
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

	return c, nil
}

// fullyClose should operate only when the state becomes CSClosed and all the
// UDP networks are closed. It writes all the remaining data read from the
// server, if in order, back to the user.
func (c *client) readLingeringMessages() {
	if c.state != CSClosed {
		panic("Connection not closed yet!")
	}
	if c.notifyCloseCaller {
		c.closeFunctionCallRes <- struct{}{}
	}
	for {
		me, found := c.receivedMessages[c.readSeqNum+1]
		if !found {
			c.readFunctionCallRes <- &PayloadError{
				nil,
				errors.New("Connection closed"),
			}
			return
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

func (c *client) MainRoutine() {
	defer c.readLingeringMessages()
	for {
		var readFunctionChan chan *PayloadError
		var pe *PayloadError

		meTemp, found := c.receivedMessages[c.readSeqNum+1]
		if !found || c.state >= CSClosing {
			readFunctionChan = nil
		} else {
			readFunctionChan = c.readFunctionCallRes
			pe = &PayloadError{meTemp.message.Payload, meTemp.err}
		}

		if c.state == CSClosed {
			c.udpConn.Close()
			clientImplLog("Closing main routine")
			return
		}

		select {
		case <-c.closeFunctionCall:
			clientImplLog("Received Close() signal")
			if c.sw.empty() {
				c.state = CSClosed
			} else {
				c.state = CSClosing
			}
			c.notifyCloseCaller = true
		case me := <-c.readMessageGeneral:
			ret := c.processReceivedMsg(&me)
			if ret == -EFATAL {
				c.udpConn.Close()
				c.state = CSClosed
				return
			}
		case readFunctionChan <- pe:
			delete(c.receivedMessages, c.readSeqNum+1)
			c.readSeqNum++
		case payload := <-c.writeFunctionCall:
			if c.state >= CSClosing {
				panic("Write() after Close()!")
			}
			err := c.handleWriteFunctionCall(payload)
			c.writeFunctionCallRes <- err
		case <-c.epochTimer.C:
			success := c.clientEpochTick()
			if !success {
				c.udpConn.Close()
				c.state = CSClosed
				return
			}
		}
	}
}

func (c *client) handleWriteFunctionCall(payload []byte) error {
	seqNum := c.sw.getSeqNum()
	writeSize := len(payload)
	checkSum := CalculateChecksum(c.connID, seqNum, writeSize, payload)
	writeMsg := NewData(c.connID, seqNum, writeSize, payload, checkSum)
	c.sw.backupUnsentMsg(writeMsg)
	return c.sendMessagefromSW(c.epochCnt)
}

// Process a message received from client ReadRoutine
// Return 0 for success, for failure
// Can only be called from client MainRoutine
func (c *client) processReceivedMsg(me *MessageError) int {
	message := me.message
	err := me.err
	if message == nil && err != nil {
		if c.state == CSInit {
			// Connection not established, client waits for server
			// until timeout
			clientImplLog("Error processing msg: " + err.Error())
			return -EAGAIN
		} else {
			clientImplFatal("Error processing msg: " + err.Error())
			return -EFATAL
		}
	}
	c.epochSinceLast = 0
	switch message.Type {
	case MsgConnect:
		panic("Client received MsgConnect!")
	case MsgData:
		if c.state == CSClosing {
			return 0
		}
		clientImplLog("Reading data message: " + message.String())
		if !c.ensureDataValidity(message) {
			clientImplLog("Discarding corrupted data message: " +
				message.String())
			// Deal as if it succeeded
			return 0
		}
		c.receivedMessages[message.SeqNum] = *me
		writeMsg := NewAck(message.ConnID, message.SeqNum)
		err := c.sendMessage(writeMsg)
		if err != nil {
			clientImplFatal("Error sending Ack to server: " +
				writeMsg.String())
			return -EFATAL
		}
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
		c.sw.ackMessage(message.SeqNum)
		if c.state == CSClosing && c.sw.empty() {
			c.state = CSClosed
			c.notifyCloseCaller = true
			return 0
		}
		err := c.sendMessagefromSW(c.epochCnt)
		if err != nil {
			clientImplFatal("Failed after receiving Ack: " +
				message.String())
			return -EFATAL
		}
	case MsgCAck:
		clientImplLog("Reading CAck message: " + message.String())
		if c.state == CSInit {
			c.connID = message.ConnID
			c.state = CSConnected
			c.connectionSuccess <- struct{}{}
			return 0
		}
		c.sw.cackMessage(message.SeqNum)
		if c.state == CSClosing && c.sw.empty() {
			c.state = CSClosed
			c.notifyCloseCaller = true
			return 0
		}
		err := c.sendMessagefromSW(c.epochCnt)
		if err != nil {
			clientImplFatal("Failed after receiving CAck: " +
				message.String())
			return -EFATAL
		}
	}
	return 0
}

// Epoch tick hitting our client,
// Return true on success, false on failure (timout, udp error, etc.)
// In the case of failure, caller should close connection
// Can only be called from client MainRoutine
func (c *client) clientEpochTick() bool {
	clientImplLog("Client epoch " + strconv.Itoa(c.epochCnt))
	c.epochCnt++
	c.epochSinceLast++
	// Detect timeout and send heartbeat if needed
	if c.epochSinceLast > c.params.EpochLimit {
		clientImplFatal("Server timeout ")
		return false
	}
	clientImplLog("Remaining epoches before server timeout: " +
		strconv.Itoa(c.params.EpochLimit-c.epochSinceLast))
	if !c.sentState { // Heartbeat
		writeMsg := NewAck(c.connID, 0)
		err := c.sendMessage(writeMsg)
		if err != nil {
			clientImplFatal("Error heartbeating to server: " +
				writeMsg.String())
			return false
		}
	}
	c.sentState = false
	// Resend all Unacked messages
	resendList := c.sw.resendMessageList(c.epochCnt)
	for _, m := range resendList {
		clientImplLog("Resending: " + m.String())
		err := c.sendMessage(m)
		if err != nil {
			clientImplFatal("Error resending: " + m.String())
			return false
		}
	}
	return true
}

func (c *client) ensureDataValidity(m *Message) bool {
	if len(m.Payload) < m.Size {
		return false
	} else if len(m.Payload) > m.Size {
		m.Payload = m.Payload[:m.Size]
	}
	if CalculateChecksum(m.ConnID, m.SeqNum, m.Size, m.Payload) != m.Checksum {
		return false
	}
	return true
}

func (c *client) ReadRoutine() {
	readRoutineState := CSInit
	for {
		if readRoutineState == CSClosed {
			clientImplFatal("ReadRoutine quiting")
			return
		}

		me := c.readMessageUDP()

		if readRoutineState == CSInit && me.err == nil {
			readRoutineState = CSConnected
		}
		if readRoutineState > CSInit && me.err != nil {
			readRoutineState = CSClosed
		}
		if readRoutineState == CSConnected ||
			readRoutineState == CSClosing {
			c.readMessageGeneral <- me
		}
	}
}

func (c *client) readMessageUDP() MessageError {
	rawMsg := make([]byte, 2048)
	var me MessageError
	me.message = nil
	n, _, err := c.udpConn.ReadFromUDP(rawMsg)
	if err != nil {
		clientImplFatal("Error reading from UDP: " + err.Error())
		me.err = err
	} else {
		err = json.Unmarshal(rawMsg[:n], &me.message)
		if err != nil {
			clientImplFatal("Error Unmarshal: " + err.Error())
			me.err = err
		}
	}
	return me
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
	if c.readFunctionAlive {
		pe := <-c.readFunctionCallRes
		if pe.err != nil {
			c.readFunctionAlive = false
		}
		return pe.payload, pe.err
	} else {
		return nil, errors.New("Connection closed")
	}
}

func (c *client) Write(payload []byte) error {
	c.writeFunctionCall <- payload
	return <-c.writeFunctionCallRes
}

func (c *client) Close() error {
	c.closeFunctionCall <- struct{}{}
	<-c.closeFunctionCallRes
	return nil
}
