// Contains the implementation of a LSP server.

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

	stopConnectionRoutine chan struct{}
	stopMainRoutine       chan struct{}
	newClientConnecting   chan *messageWithAddress
	newAck                chan Message
	newCAck               chan Message
	newDataReceiving      chan Message

	readFunctionReady chan Message
}

type messageWithAddress struct {
	message Message
	addr    *lspnet.UDPAddr
}

type clientInfo struct {
	connID int
	addr   string

	sn int
}

func newClientInfo(connID int, addr string, sn int) *clientInfo {
	return &clientInfo{
		connID: connID,
		addr:   addr,

		sn: sn,
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
		stopConnectionRoutine: make(chan struct{}),
		stopMainRoutine:       make(chan struct{}),
		newClientConnecting:   make(chan *messageWithAddress),
		newAck:                make(chan Message),
		newCAck:               make(chan Message),
		newDataReceiving:      make(chan Message),
		readFunctionReady:     make(chan Message),
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
			var b []byte
			if _, addr, err := s.udpConn.ReadFromUDP(b); err != nil {
				return
			}
			var message Message
			if err := json.Unmarshal(b, &message); err != nil {
			}
			switch message.Type {
			case MsgConnect:
				s.newClientConnecting <- &messageWithAddress{message, addr}
			case MsgAck:
				s.newAck <- message
			case MsgCAck:
				s.newCAck <- message
			case MsgData:
				s.newDataReceiving <- message
			}
		}
	}
}

func (s *server) MainRoutine() {
	for {
		select {
		case <-s.stopMainRoutine:
			return
		case mwa := <-s.newClientConnecting:
			if _, ok := s.clientsAddr[mwa.addr.String()]; !ok {
				s.clientsCnt++
				s.clientsID[s.clientsCnt] = newClientInfo(s.clientsCnt, mwa.addr.String(), mwa.message.SeqNum)
				s.clientsAddr[mwa.addr.String()] = s.clientsID[s.clientsCnt]
			}
			// write back Ack
		case mwa := <-s.newAck:
			// do something with sliding window
		case mwa := <-s.newCAck:
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
			//cInfo := s.clientsID[message.ConnID]
			// sliding window check and recv
			// write back Ack
			s.readFunctionReady <- message
		}
	}
}

func (s *server) Read() (int, []byte, error) {
	select {
	case message := <-s.readFunctionReady:
		return message.ConnID, message.Payload, nil
	}
}

func (s *server) Write(connId int, payload []byte) error {
	return errors.New("not yet implemented")
}

func (s *server) CloseConn(connId int) error {
	return errors.New("not yet implemented")
}

func (s *server) Close() error {
	s.stopMainRoutine <- struct{}{}
	s.stopConnectionRoutine <- struct{}{}
	return errors.New("not yet implemented")
}
