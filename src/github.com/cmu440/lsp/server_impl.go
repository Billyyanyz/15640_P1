// Contains the implementation of an LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/cmu440/lspnet"
)

type server struct {
	params      *Params
	udpConn     *lspnet.UDPConn
	clientsID   map[int]*clientInfo
	clientsAddr map[string]*clientInfo
	clientsCnt  int

	newClientConnecting chan messageWithAddress
	newAck              chan *Message
	newCAck             chan *Message
	newDataReceiving    chan *Message

	readFunctionCall    chan struct{}
	readFunctionCallRes chan *Message
	readyForReadMsg     []*Message
	await               bool

	checkIDCall       chan int
	checkIDCallRes    chan bool
	writeFunctionCall chan *Message
	writeAckCall      chan *Message

	epochTimer *time.Ticker
	epochCnt   int

	closeClientCall     chan int
	pendingCloseClients map[int]bool

	closeFunctionCall    chan struct{}
	closeFunctionCallRes chan struct{}
	pendingClose         bool
	serverClosed         bool
	stopMain             chan struct{}
}

type messageWithAddress struct {
	message *Message
	addr    *lspnet.UDPAddr
}

type clientInfo struct {
	connID int
	addr   *lspnet.UDPAddr

	buffRecv  bufferedReceiver
	slideSndr slidingWindowSender

	activeWroteInEpoch  bool
	activeReadFromEpoch bool
	lastReadEpoch       int
}

func (s *server) newClientInfo(connID int, addr *lspnet.UDPAddr, sn int) *clientInfo {
	return &clientInfo{
		connID: connID,
		addr:   addr,

		buffRecv:  newBufferedReceiver(sn),
		slideSndr: newSlidingWindowSender(sn, s.params.WindowSize, s.params.MaxUnackedMessages, s.params.MaxBackOffInterval),

		activeWroteInEpoch:  false,
		activeReadFromEpoch: true,
		lastReadEpoch:       0,
	}

}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	s := &server{
		params:      params,
		clientsID:   make(map[int]*clientInfo),
		clientsAddr: make(map[string]*clientInfo),
		clientsCnt:  0,

		newClientConnecting: make(chan messageWithAddress),
		newAck:              make(chan *Message),
		newCAck:             make(chan *Message),
		newDataReceiving:    make(chan *Message),

		readFunctionCall:    make(chan struct{}),
		readFunctionCallRes: make(chan *Message),
		readyForReadMsg:     make([]*Message, 0, 10),
		await:               false,

		checkIDCall:       make(chan int),
		checkIDCallRes:    make(chan bool),
		writeFunctionCall: make(chan *Message, 1),
		writeAckCall:      make(chan *Message),

		epochTimer: time.NewTicker(time.Millisecond *
			time.Duration(params.EpochMillis)),
		epochCnt: 0,

		closeClientCall:     make(chan int),
		pendingCloseClients: make(map[int]bool),

		closeFunctionCall:    make(chan struct{}),
		closeFunctionCallRes: make(chan struct{}),
		pendingClose:         false,
		serverClosed:         false,
		stopMain:             make(chan struct{}, 1),
	}
	var addr *lspnet.UDPAddr
	var err error
	if addr, err = lspnet.ResolveUDPAddr("udp",
		lspnet.JoinHostPort("localhost", strconv.Itoa(port))); err != nil {
		return nil, err
	}
	var conn *lspnet.UDPConn
	if conn, err = lspnet.ListenUDP("udp", addr); err != nil {
		return nil, err
	}
	s.udpConn = conn
	go s.ReadRoutine()
	go s.MainRoutine()
	return s, nil
}

func (s *server) ReadRoutine() {
	for {
		b := make([]byte, 2048)
		n, addr, err := s.udpConn.ReadFromUDP(b)
		if err != nil {
			return
		}
		var m Message
		if err = json.Unmarshal(b[:n], &m); err != nil {
			continue
		}
		switch m.Type {
		case MsgConnect:
			s.newClientConnecting <- messageWithAddress{&m, addr}
		case MsgAck:
			s.newAck <- &m
		case MsgCAck:
			s.newCAck <- &m
		case MsgData:
			s.newDataReceiving <- &m
		}
	}
}

func (s *server) MainRoutine() {
	for {
		select {
		case <-s.stopMain:
			return

		case mwa := <-s.newClientConnecting:
			s.handleConnect(mwa.message, mwa.addr)
		case m := <-s.newAck:
			s.handleAck(m)
		case m := <-s.newCAck:
			s.handleCAck(m)
		case m := <-s.newDataReceiving:
			s.handleData(m)

		case id := <-s.checkIDCall:
			_, ok := s.clientsID[id]
			s.checkIDCallRes <- ok

		case <-s.readFunctionCall:
			if len(s.readyForReadMsg) == 0 {
				s.await = true
			} else {
				s.readFunctionCallRes <- s.readyForReadMsg[0]
				s.readyForReadMsg = s.readyForReadMsg[1:]
			}

		case m := <-s.writeFunctionCall:
			cInfo := s.clientsID[m.ConnID]
			m.SeqNum = cInfo.slideSndr.getSeqNum()
			m.Size = len(m.Payload)
			m.Checksum = CalculateChecksum(m.ConnID, m.SeqNum, m.Size, m.Payload)
			cInfo.slideSndr.backupUnsentMsg(m)
			s.checkSendMsg(m.ConnID)

		case <-s.epochTimer.C:
			s.epochCnt++
			s.checkConnActivity()
			s.resendUnackedMessage()

		case id := <-s.closeClientCall:
			s.pendingCloseClients[id] = true
			if s.clientsID[id].slideSndr.empty() {
				s.closeClientNow(id)
			}
			if s.pendingClose {
				s.attemptClosingServer()
			}

		case <-s.closeFunctionCall:
			s.pendingClose = true
			s.attemptClosingServer()
		}
	}
}

func (s *server) writeMsg(m *Message, id int) {
	cInfo := s.clientsID[id]
	b, err := json.Marshal(m)
	if err != nil {
		return
	}
	if _, err = s.udpConn.WriteToUDP(b, cInfo.addr); err != nil {
		return
	}
}

func (s *server) writeBackAck(id int, seqNum int) {
	retMessage := NewAck(id, seqNum)
	s.writeMsg(retMessage, id)
}

func (s *server) handleConnect(m *Message, addr *lspnet.UDPAddr) {
	if _, ok := s.clientsAddr[addr.String()]; !ok {
		s.clientsCnt++
		s.clientsID[s.clientsCnt] = s.newClientInfo(s.clientsCnt, addr, m.SeqNum)
		cInfo := s.clientsID[s.clientsCnt]
		s.clientsAddr[addr.String()] = cInfo
		s.pendingCloseClients[s.clientsCnt] = false
		s.writeBackAck(cInfo.connID, m.SeqNum)
	}
}

func (s *server) handleAck(m *Message) {
	if cInfo, ok := s.clientsID[m.ConnID]; ok {
		cInfo.activeReadFromEpoch = true
		if m.SeqNum > 0 {
			cInfo.slideSndr.ackMessage(m.SeqNum)
			s.checkSendMsg(m.ConnID)
			if cInfo.slideSndr.empty() {
				if s.pendingCloseClients[m.ConnID] {
					s.closeClientNow(m.ConnID)
				}
				if s.pendingClose {
					s.attemptClosingServer()
				}
			}
		}
	}
}

func (s *server) handleCAck(m *Message) {
	if cInfo, ok := s.clientsID[m.ConnID]; ok {
		cInfo.activeReadFromEpoch = true
		if m.SeqNum > 0 {
			cInfo.slideSndr.cackMessage(m.SeqNum)
			s.checkSendMsg(m.ConnID)
			if cInfo.slideSndr.empty() {
				if s.pendingCloseClients[m.ConnID] {
					s.closeClientNow(m.ConnID)
				}
				if s.pendingClose {
					s.attemptClosingServer()
				}
			}
		}
	}
}

func (s *server) ensureDataValidity(m *Message) bool {
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

func (s *server) handleData(m *Message) {
	if cInfo, ok := s.clientsID[m.ConnID]; ok {
		cInfo.activeReadFromEpoch = true
		if !s.ensureDataValidity(m) {
			return
		}
		cInfo.buffRecv.recvMsg(m)

		for cInfo.buffRecv.readyToRead() {
			s.readyForReadMsg = append(s.readyForReadMsg, cInfo.buffRecv.deliverToRead())
		}
		if s.await && len(s.readyForReadMsg) != 0 {
			s.readFunctionCallRes <- s.readyForReadMsg[0]
			s.readyForReadMsg = s.readyForReadMsg[1:]
			s.await = false
		}
		s.writeBackAck(m.ConnID, m.SeqNum)
	}
}

func (s *server) checkSendMsg(id int) {
	cInfo := s.clientsID[id]
	if _, m := cInfo.slideSndr.nextMsgToSend(); m != nil {
		cInfo.activeWroteInEpoch = true
		s.writeMsg(m, id)
		cInfo.slideSndr.markNextMessageSent(m, s.epochCnt)
	}
}

func (s *server) checkConnActivity() {
	for id, cInfo := range s.clientsID {
		if !cInfo.activeWroteInEpoch {
			s.writeBackAck(id, 0)
		}
		if !cInfo.activeReadFromEpoch {
			cInfo.lastReadEpoch++
			if cInfo.lastReadEpoch >= s.params.EpochLimit {
				s.closeClientNow(id)
			}
		} else {
			cInfo.lastReadEpoch = 0
		}
		cInfo.activeWroteInEpoch = false
		cInfo.activeReadFromEpoch = false
	}
}

func (s *server) resendUnackedMessage() {
	for id, cInfo := range s.clientsID {
		resendMessageList := cInfo.slideSndr.resendMessageList(s.epochCnt)
		for _, m := range resendMessageList {
			serverImplLog("Resending " + m.String())
			s.writeMsg(m, id)
		}
	}
}

func (s *server) closeClientNow(id int) {
	delete(s.clientsAddr, s.clientsID[id].addr.String())
	delete(s.pendingCloseClients, id)
	delete(s.clientsID, id)

	s.readyForReadMsg = append(s.readyForReadMsg, &Message{Type: MsgAck, ConnID: id})
	if s.await {
		s.readFunctionCallRes <- s.readyForReadMsg[0]
		s.readyForReadMsg = s.readyForReadMsg[1:]
		s.await = false
	}
}

func (s *server) attemptClosingServer() {
	emptyPending := true
	for id := range s.clientsID {
		if !s.clientsID[id].slideSndr.empty() {
			emptyPending = false
		}
	}
	if emptyPending {
		s.pendingClose = false
		s.serverClosed = true
		if err := s.udpConn.Close(); err != nil {
		}
		s.stopMain <- struct{}{}
		s.closeFunctionCallRes <- struct{}{}
	}
}

func (s *server) Read() (int, []byte, error) {
	s.readFunctionCall <- struct{}{}
	m := <-s.readFunctionCallRes
	switch m.Type {
	case MsgAck:
		return m.ConnID, nil, errors.New("CONNECTION with client id: " + strconv.Itoa(m.ConnID) + " is closed")
	default:
		return m.ConnID, m.Payload, nil
	}
}

func (s *server) Write(connId int, payload []byte) error {
	s.checkIDCall <- connId
	res := <-s.checkIDCallRes
	if !res {
		return errors.New("client ID does not exist")
	} else {
		s.writeFunctionCall <- NewData(connId, 0, 0, payload, 0)
		return nil
	}
}

func (s *server) CloseConn(connId int) error {
	s.checkIDCall <- connId
	res := <-s.checkIDCallRes
	if !res {
		return errors.New("client ID does not exist")
	} else {
		s.closeClientCall <- connId
		return nil
	}
}

func (s *server) Close() error {
	s.closeFunctionCall <- struct{}{}
	<-s.closeFunctionCallRes
	return nil
}
