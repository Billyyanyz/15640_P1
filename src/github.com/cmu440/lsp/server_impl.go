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
	readFunctionCallRes chan messageWithErrID
	checkIDCall         chan int
	checkIDCallRes      chan bool
	attemptWriting      chan int
	pendingMessages     map[int]chan *Message
	closeClient         chan int

	closeFunctionCall chan struct{}
	attemptClosing    chan struct{}
	stopConnection    chan struct{}
	stopMain          chan struct{}
}

type messageWithAddress struct {
	message *Message
	addr    *lspnet.UDPAddr
}

type messageWithErrID struct {
	message *Message
	err_id  int
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
		readFunctionCallRes: make(chan messageWithErrID),
		checkIDCall:         make(chan int),
		checkIDCallRes:      make(chan bool),
		attemptWriting:      make(chan int),
		pendingMessages:     make(map[int]chan *Message),
		closeClient:         make(chan int),

		closeFunctionCall: make(chan struct{}),
		attemptClosing:    make(chan struct{}),
		stopConnection:    make(chan struct{}),
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
	go s.connectionRoutine()
	go s.MainRoutine()
	return s, nil
}

func (s *server) connectionRoutine() {
	for {
		select {
		case <-s.stopConnection:
			return
		default:
			var b []byte
			n, addr, err := s.udpConn.ReadFromUDP(b)
			if err != nil {
				return
			}
			var message Message
			if err = json.Unmarshal(b[:n], &message); err != nil {
				continue
			}
			switch message.Type {
			case MsgConnect:
				s.newClientConnecting <- messageWithAddress{&message, addr}
			case MsgAck:
				s.newAck <- &message
			case MsgCAck:
				s.newCAck <- &message
			case MsgData:
				s.newDataReceiving <- &message
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
		// when ack, send a signal to check if all buffer is empty, if so set serverclosed=true
		// do only when pendingclose=true
		case mwa := <-s.newClientConnecting:
			if _, ok := s.clientsAddr[mwa.addr.String()]; !ok {
				s.clientsCnt++
				s.clientsID[s.clientsCnt] = s.newClientInfo(s.clientsCnt, mwa.addr, mwa.message.SeqNum+1)
				s.clientsAddr[mwa.addr.String()] = s.clientsID[s.clientsCnt]
				s.pendingMessages[s.clientsCnt] = make(chan *Message)
			}
			// write back Ack
		case message := <-s.newAck:
			cInfo := s.clientsID[message.ConnID]
			cInfo.slideSndr.ackMessage(message.SeqNum)
			if s.pendingClose {
				go func() {
					s.attemptClosing <- struct{}{}
				}()
			}
		case message := <-s.newCAck:
			cInfo := s.clientsID[message.ConnID]
			cInfo.slideSndr.cackMessage(message.SeqNum)
			if s.pendingClose {
				go func() {
					s.attemptClosing <- struct{}{}
				}()
			}
		case message := <-s.newDataReceiving:
			cInfo := s.clientsID[message.ConnID]
			if cInfo.closed {
				continue
			}
			if len(message.Payload) > message.Size {
				continue
			} else if len(message.Payload) < message.Size {
				message.Payload = message.Payload[:message.Size]
			}
			if CalculateChecksum(message.ConnID, message.SeqNum, message.Size, message.Payload) !=
				message.Checksum {
				continue
			}
			cInfo.buffRecv.recvMsg(message)
			var retMessage Message
			retMessage.Type = MsgAck
			retMessage.ConnID = message.ConnID
			retMessage.SeqNum = message.SeqNum
			go func() {
				s.attemptWriting <- message.ConnID
				s.pendingMessages[message.ConnID] <- &retMessage
			}()
		case <-s.readFunctionCall:
			if s.serverClosed {
				s.readFunctionCallRes <- messageWithErrID{nil, -1}
				continue
			}
			for cID := range s.clientsID {
				cInfo := s.clientsID[cID]
				if !cInfo.closed && cInfo.buffRecv.readyToRead() {
					readRes := messageWithErrID{cInfo.buffRecv.deliverToRead(), 0}
					s.readFunctionCallRes <- readRes
				}
			}
		case id := <-s.attemptWriting:
			message := <-s.pendingMessages[id]
			cInfo := s.clientsID[id]
			if message.Type == MsgData {
				if !cInfo.slideSndr.readyToSend() {
					go func() {
						s.attemptWriting <- id
						s.pendingMessages[id] <- message
					}()
					continue
				}
				message.SeqNum = cInfo.slideSndr.getSeriesNum()
				message.Size = len(message.Payload)
				message.Checksum = CalculateChecksum(id, message.SeqNum, message.Size, message.Payload)
			}
			if b, err := json.Marshal(message); err == nil {
				if _, err := s.udpConn.WriteToUDP(b, cInfo.addr); err == nil {
				}
			}
		case id := <-s.closeClient:
			s.clientsID[id].closed = true
			go func() {
				s.readFunctionCallRes <- messageWithErrID{&Message{ConnID: id}, -2}
			}()
		case <-s.attemptClosing:
			emptyPending := true
			for id, _ := range s.clientsID {
				if !(s.clientsID[id].closed || s.clientsID[id].slideSndr.empty()) {
					emptyPending = false
				}
			}
			if emptyPending {
				s.serverClosed = true
				s.stopConnection <- struct{}{}
				s.stopMain <- struct{}{}
			}
		}
	}
}

func (s *server) Read() (int, []byte, error) {
	s.readFunctionCall <- struct{}{}
	res := <-s.readFunctionCallRes
	if res.err_id == -1 {
		return -1, nil, errors.New("Server is closed")
	}
	if res.err_id == -2 {
		return -1, nil, errors.New("Connection with client id: " + strconv.Itoa(res.message.ConnID) + " is closed")
	}
	return res.message.ConnID, res.message.Payload, nil
}

func (s *server) Write(connId int, payload []byte) error {
	s.checkIDCall <- connId
	if res := <-s.checkIDCallRes; !res {
		return errors.New("client ID does not exist")
	}
	var message Message
	message.Type = MsgData
	message.ConnID = connId
	message.Payload = payload
	go func() {
		s.attemptWriting <- connId
		s.pendingMessages[connId] <- &message
	}()
	return nil
}

func (s *server) CloseConn(connId int) error {
	s.checkIDCall <- connId
	if res := <-s.checkIDCallRes; !res {
		return errors.New("client ID does not exist")
	}
	s.closeClient <- connId
	return nil
}

func (s *server) Close() error {
	s.closeFunctionCall <- struct{}{}
	return nil
}
