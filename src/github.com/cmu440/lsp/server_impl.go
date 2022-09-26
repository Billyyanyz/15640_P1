// Contains the implementation of an LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"strconv"
)

type server struct {
	params       *Params
	udpConn      *lspnet.UDPConn
	clientsID    map[int]*clientInfo
	clientsAddr  map[string]*clientInfo
	clientsCnt   int
	pendingClose bool
	serverClosed bool

	newClientConnecting chan messageWithAddress
	newAck              chan *Message
	newCAck             chan *Message
	newDataReceiving    chan *Message

	readFunctionCall    chan struct{}
	readyForReadMsg     []*Message
	await               bool
	readFunctionCallRes chan *Message
	writeFunctionCall   chan *Message
	writeAckCall        chan *Message
	checkIDCall         chan int
	checkIDCallRes      chan bool
	closeClient         chan int
	closeClientToRead   chan int

	closeFunctionCall chan struct{}
	attemptClosing    chan struct{}
	stopReadRoutine   chan struct{}
	stopMain          chan struct{}
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

	closed bool
}

func (s *server) newClientInfo(connID int, addr *lspnet.UDPAddr, sn int) *clientInfo {
	return &clientInfo{
		connID: connID,
		addr:   addr,

		buffRecv:  newBufferedReceiver(sn),
		slideSndr: newSlidingWindowSender(sn, s.params.WindowSize, s.params.MaxUnackedMessages),

		closed: false,
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
		params:       params,
		clientsID:    make(map[int]*clientInfo),
		clientsAddr:  make(map[string]*clientInfo),
		clientsCnt:   0,
		pendingClose: false,
		serverClosed: false,

		newClientConnecting: make(chan messageWithAddress),
		newAck:              make(chan *Message),
		newCAck:             make(chan *Message),
		newDataReceiving:    make(chan *Message),

		readFunctionCall:    make(chan struct{}),
		readyForReadMsg:     make([]*Message, 0, 10),
		await:               false,
		readFunctionCallRes: make(chan *Message),
		writeFunctionCall:   make(chan *Message, 1),
		writeAckCall:        make(chan *Message),
		checkIDCall:         make(chan int),
		checkIDCallRes:      make(chan bool),
		closeClient:         make(chan int),

		closeFunctionCall: make(chan struct{}),
		attemptClosing:    make(chan struct{}),
		stopReadRoutine:   make(chan struct{}),
		stopMain:          make(chan struct{}),
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
		select {
		case <-s.stopReadRoutine:
			return
		default:
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
}

func (s *server) MainRoutine() {
	for {
		select {
		case <-s.stopMain:
			return
		case <-s.closeFunctionCall:
			s.pendingClose = true

		case mwa := <-s.newClientConnecting:
			s.handleConnect(mwa.message, mwa.addr)
		case m := <-s.newAck:
			s.handleCAck(m)
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

		case id := <-s.closeClient:
			s.clientsID[id].closed = true
			go func() {
				s.closeClientToRead <- id
			}()

		case <-s.attemptClosing:
			emptyPending := true
			for id := range s.clientsID {
				if !(s.clientsID[id].closed || s.clientsID[id].slideSndr.empty()) {
					emptyPending = false
				}
			}
			if emptyPending {
				s.serverClosed = true
				s.stopReadRoutine <- struct{}{}
				s.stopMain <- struct{}{}
			}

		}
	}
}

func (s *server) ackWriteBack(id int, seqNum int) {
	retMessage := NewAck(id, seqNum)
	b, err := json.Marshal(retMessage)
	if err != nil {
		return
	}
	if _, err = s.udpConn.WriteToUDP(b, s.clientsID[id].addr); err != nil {
		return
	}
}

func (s *server) handleConnect(m *Message, addr *lspnet.UDPAddr) {
	if _, ok := s.clientsAddr[addr.String()]; !ok {
		s.clientsCnt++
		s.clientsID[s.clientsCnt] = s.newClientInfo(s.clientsCnt, addr, m.SeqNum)
		cInfo := s.clientsID[s.clientsCnt]
		s.clientsAddr[addr.String()] = cInfo
		s.ackWriteBack(cInfo.connID, m.SeqNum)
	}
}

func (s *server) handleAck(m *Message) {
	cInfo := s.clientsID[m.ConnID]
	cInfo.slideSndr.ackMessage(m.SeqNum)
	s.checkSendMsg(m.ConnID)
	if s.pendingClose {
		go func() {
			s.attemptClosing <- struct{}{}
		}()
	}
}

func (s *server) handleCAck(m *Message) {
	cInfo := s.clientsID[m.ConnID]
	cInfo.slideSndr.cackMessage(m.SeqNum)
	s.checkSendMsg(m.ConnID)
	if s.pendingClose {
		go func() {
			s.attemptClosing <- struct{}{}
		}()
	}
}

func (s *server) ensureDataValidity(m *Message) bool {
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

func (s *server) handleData(m *Message) {
	cInfo := s.clientsID[m.ConnID]
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
	s.ackWriteBack(m.ConnID, m.SeqNum)
}

func (s *server) checkSendMsg(id int) {
	cInfo := s.clientsID[id]
	if _, m := cInfo.slideSndr.nextMsgToSend(); m != nil {
		b, err := json.Marshal(m)
		if err != nil {
			return
		}
		if _, err = s.udpConn.WriteToUDP(b, cInfo.addr); err != nil {
			return
		}
		cInfo.slideSndr.markMessageSent(m)
	}
}

func (s *server) Read() (int, []byte, error) {
	select {
	case id := <-s.closeClientToRead:
		return -1, nil, errors.New("connection with client id: " + strconv.Itoa(id) + " is closed")
	default:
		s.readFunctionCall <- struct{}{}
		m := <-s.readFunctionCallRes
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
		s.closeClient <- connId
		// TODO: select in Read to handle closed connection
		return nil
	}
}

func (s *server) Close() error {
	s.closeFunctionCall <- struct{}{}
	return nil
}
