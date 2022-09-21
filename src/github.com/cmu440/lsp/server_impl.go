// Contains the implementation of an LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"strconv"
)

type server struct {
	params      *Params
	udpConn     *lspnet.UDPConn
	clientsID   map[int]*clientInfo
	clientsAddr map[string]*clientInfo
	clientsCnt  int
	closed      bool

	newClientConnecting chan messageWithAddress
	newAck              chan *Message
	newCAck             chan *Message
	newDataReceiving    chan *Message

	readFunctionCall    chan struct{}
	readFunctionCallRes chan messageWithID
	closeClient         chan int
	closeClientRes      chan bool

	stopConnectionRoutine chan struct{}
	stopMainRoutine       chan struct{}
}

type messageWithAddress struct {
	message *Message
	addr    *lspnet.UDPAddr
}

type messageWithID struct {
	message *Message
	id      int
}

type clientInfo struct {
	connID int
	addr   string

	window slidingWindowReceiver

	closed bool
}

func (s *server) newClientInfo(connID int, addr string, sn int) *clientInfo {
	return &clientInfo{
		connID: connID,
		addr:   addr,

		window: newSlidingWindowReceiver(sn, s.params.WindowSize, s.params.MaxUnackedMessages),

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
		params:                params,
		clientsID:             make(map[int]*clientInfo),
		clientsAddr:           make(map[string]*clientInfo),
		clientsCnt:            0,
		closed:                false,
		newClientConnecting:   make(chan messageWithAddress),
		newAck:                make(chan *Message),
		newCAck:               make(chan *Message),
		newDataReceiving:      make(chan *Message),
		readFunctionCall:      make(chan struct{}),
		readFunctionCallRes:   make(chan messageWithID),
		closeClient:           make(chan int),
		closeClientRes:        make(chan bool),
		stopConnectionRoutine: make(chan struct{}),
		stopMainRoutine:       make(chan struct{}),
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
		case <-s.stopConnectionRoutine:
			return
		default:
			var addr *lspnet.UDPAddr
			var err error
			var b []byte
			if _, addr, err = s.udpConn.ReadFromUDP(b); err != nil {
				return
			}
			var message Message
			if err = json.Unmarshal(b, &message); err != nil {
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
		case <-s.stopMainRoutine:
			s.closed = true
			return
		case mwa := <-s.newClientConnecting:
			if _, ok := s.clientsAddr[mwa.addr.String()]; !ok {
				s.clientsCnt++
				s.clientsID[s.clientsCnt] = s.newClientInfo(s.clientsCnt, mwa.addr.String(), mwa.message.SeqNum)
				s.clientsAddr[mwa.addr.String()] = s.clientsID[s.clientsCnt]
			}
			// write back Ack
		case message := <-s.newAck:
			// do something with sliding window
		case message := <-s.newCAck:
			// do something with sliding window
		case message := <-s.newDataReceiving:
			if len(message.Payload) > message.Size {
				continue
			} else if len(message.Payload) < message.Size {
				message.Payload = message.Payload[:message.Size]
			}
			if CalculateChecksum(message.ConnID, message.SeqNum, message.Size, message.Payload) !=
				message.Checksum {
				continue
			}
			cInfo := s.clientsID[message.ConnID]
			if cInfo.window.outsideWindow(message) {
				continue
			}
			cInfo.window.recvMsg(message)

			// when read calls, return only the lowest possible one
			// lowest possible one uses size1 buffer to mark readiness
			// write back Ack
		case <-s.readFunctionCall:
			if s.closed {
				s.readFunctionCallRes <- messageWithID{nil, -1}
			}

		case id := <-s.closeClient:
			if _, ok := s.clientsID[id]; !ok {
				s.closeClientRes <- false
			} else {
				s.clientsID[id].closed = true
				s.closeClientRes <- true
			}
			// also notify read function this!
		}
	}
}

func (s *server) Read() (int, []byte, error) {
	s.readFunctionCall <- struct{}{}
	res := <-s.readFunctionCallRes
	if res.id == -1 {
		return -1, nil, errors.New("Server is closed")
	}
	// send read request to main
	// then main check if server closed, client closed, etc.
	// if not, return a signal to this function containing the info
	// see closeConn for template, send things to mainRoutine, then wait for results to get error
	return -1, nil, nil
}

func (s *server) Write(connId int, payload []byte) error {
	return errors.New("not yet implemented")
}

func (s *server) CloseConn(connId int) error {
	s.closeClient <- connId
	res := <-s.closeClientRes
	if !res {
		return errors.New("client ID does not exist")
	}
	return nil
}

func (s *server) Close() error {
	// but how to block until pending messages?
	// wait until a signal send to close marking all message solved
	s.stopConnectionRoutine <- struct{}{}
	s.stopMainRoutine <- struct{}{}
	return nil
}
