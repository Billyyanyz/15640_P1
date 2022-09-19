// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"strconv"
)

type server struct {
	udpConn lspnet.UDPConn

	stopConnectionRoutine chan bool
	stopMainRoutine       chan bool
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	s := &server{
		stopConnectionRoutine: make(chan bool),
	}
	if addr, err := lspnet.ResolveUDPAddr("udp",
		lspnet.JoinHostPort("localhost", strconv.Itoa(port))); err != nil {
		return nil, err
	} else {
		if conn, err := lspnet.ListenUDP("udp", addr); err != nil {
			return nil, err
		} else {
			s.udpConn = *conn
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
			if n, addr, err := s.udpConn.ReadFromUDP(b); err != nil {
				return
			} else {
				bTrim := b[:n]
				var message Message
				if err := json.Unmarshal(bTrim, &message); err != nil {
				} else {
					switch message.Type {
					case MsgConnect:

					case MsgAck:

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
	s.stopConnectionRoutine <- true
	s.stopMainRoutine <- true
	return errors.New("not yet implemented")
}
