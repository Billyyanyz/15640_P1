// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"strconv"
)

type server struct {
	udpConn     *lspnet.UDPConn
	clientsID   map[int]*clientInfo
	clientsAddr map[string]*clientInfo
	clientsCnt  int

	stopConnectionRoutine chan bool
	stopMainRoutine       chan bool
	newClientConnecting   chan messageWithAddress
	newAck                chan messageWithAddress
}

type messageWithAddress struct {
	message Message
	addr    *lspnet.UDPAddr
}

type clientInfo struct {
	connID int
	addr   string
}

func newClientInfo(connID int, addr string) *clientInfo {
	return &clientInfo{
		connID: connID,
		addr:   addr,
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
		clientsID:             make(map[int]*clientInfo),
		clientsAddr:           make(map[string]*clientInfo),
		clientsCnt:            0,
		stopConnectionRoutine: make(chan bool),
		stopMainRoutine:       make(chan bool),
		newClientConnecting:   make(chan messageWithAddress),
	}
	if addr, err := lspnet.ResolveUDPAddr("udp",
		lspnet.JoinHostPort("localhost", strconv.Itoa(port))); err != nil {
		return nil, err
	} else {
		if conn, err := lspnet.ListenUDP("udp", addr); err != nil {
			return nil, err
		} else {
			s.udpConn = conn
			go s.connectionRoutine()
			go s.MainRoutine()
		}
	}
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
			} else {
				var message Message
				if err := json.Unmarshal(b, &message); err != nil {
				} else {
					switch message.Type {
					case MsgConnect:
						s.newClientConnecting <- messageWithAddress{message, addr}
					case MsgAck:
						s.newAck <- messageWithAddress{message, addr}
					case MsgCAck:

					case MsgData:
					}
				}

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
				s.clientsID[s.clientsCnt] = newClientInfo(s.clientsCnt, mwa.addr.String())
				s.clientsAddr[mwa.addr.String()] = s.clientsID[s.clientsCnt]
			}
			// write back Ack
		case mwa := <-s.newAck:

		}
	}
}

func (s *server) Read() (int, []byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	select {} // Blocks indefinitely.
	return -1, nil, errors.New("not yet implemented")
}

func (s *server) Write(connId int, payload []byte) error {
	return errors.New("not yet implemented")
}

func (s *server) CloseConn(connId int) error {
	return errors.New("not yet implemented")
}

func (s *server) Close() error {
	s.stopMainRoutine <- true
	s.stopConnectionRoutine <- true
	return errors.New("not yet implemented")
}
