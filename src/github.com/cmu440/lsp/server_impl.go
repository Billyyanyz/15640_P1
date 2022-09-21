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
	readFunctionCallRes chan messageWithErrID
	writeFunctionCall   chan *Message
	closeClient         chan int
	closeClientRes      chan bool

	stopConnectionRoutine chan struct{}
	stopMainRoutine       chan struct{}
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

	buffRecv bufferedReceiver

	closed bool
}

func (s *server) newClientInfo(connID int, addr *lspnet.UDPAddr, sn int) *clientInfo {
	return &clientInfo{
		connID: connID,
		addr:   addr,

		buffRecv: newBufferedReceiver(sn),

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
		readFunctionCallRes:   make(chan messageWithErrID),
		writeFunctionCall:     make(chan *Message),
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
		case <-s.stopMainRoutine:
			s.closed = true
			return
		case mwa := <-s.newClientConnecting:
			if _, ok := s.clientsAddr[mwa.addr.String()]; !ok {
				s.clientsCnt++
				s.clientsID[s.clientsCnt] = s.newClientInfo(s.clientsCnt, mwa.addr, mwa.message.SeqNum+1)
				s.clientsAddr[mwa.addr.String()] = s.clientsID[s.clientsCnt]
			}
			// write back Ack
		case message := <-s.newAck:
			// do something with sliding window
		case message := <-s.newCAck:
			// do something with sliding window
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
			// write back Ack
		case <-s.readFunctionCall:
			if s.closed {
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
		case m := <-s.writeFunctionCall:
			m.Type = MsgData
			//get sequence number
			//m.SeqNum <-
			//also check the window!
			m.Size = len(m.Payload)
			m.Checksum = CalculateChecksum(m.ConnID, m.SeqNum, m.Size, m.Payload)
			var b []byte
			var err error
			if b, err = json.Marshal(m); err != nil {
				continue
			}
			if _, err = s.udpConn.WriteToUDP(b, s.clientsID[m.ConnID].addr); err != nil {
				continue
			}

		case id := <-s.closeClient:
			if _, ok := s.clientsID[id]; !ok {
				s.closeClientRes <- false
			} else {
				s.clientsID[id].closed = true
				go func() {
					s.readFunctionCallRes <- messageWithErrID{&Message{ConnID: id}, -2}
				}()
				s.closeClientRes <- true
			}
			// also notify read function this!
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
	var message Message
	message.ConnID = connId
	message.Payload = payload
	s.writeFunctionCall <- &message
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
