package lsp

import (
	"errors"
)

type slidingWindowSender struct {
	// [l, minUnsentSN) are messages sent but not Ack'ed
	// [minUnsentSN, nextSN) are messages requested to send but not sent
	// Messages >= minUnSentSN may be eligible to send, eligibility should
	// be checked by the client and server's write routine by calling
	// readyToSend()
	l             int
	minUnsentSN   int
	nextSN        int
	size          int
	unAckedList   map[int]*Message // [l, minUnsentSN)
	unsentData    map[int]*Message // [minUnsentSN, nextSN)
	maxUnackedMsg int
}

// Must be called AFTER the handshake is complete
func newSlidingWindowSender(sn int, windowSize int, maxUnackedMsg int) slidingWindowSender {
	s := slidingWindowSender{
		l:             sn + 1,
		minUnsentSN:   sn + 1,
		nextSN:        sn + 1,
		size:          windowSize,
		unAckedList:   make(map[int]*Message),
		unsentData:    make(map[int]*Message),
		maxUnackedMsg: maxUnackedMsg,
	}
	return s
}

func (w *slidingWindowSender) nextMsgToSend() (int, *Message) {
	if len(w.unsentData) == 0 {
		return -1, nil
	}
	if len(w.unAckedList) >= w.maxUnackedMsg {
		return -2, nil
	}
	if w.minUnsentSN >= w.l+w.size {
		return -3, nil
	}
	return w.minUnsentSN, w.unsentData[w.minUnsentSN]
}

func (w *slidingWindowSender) getSeqNum() int {
	res := w.nextSN
	w.nextSN++
	return res
}

func (w *slidingWindowSender) backupUnsentMsg(m *Message) {
	w.unsentData[m.SeqNum] = m
}

func (w *slidingWindowSender) markMessageSent(m *Message) error {
	sn := m.SeqNum
	if sn != w.minUnsentSN {
		return errors.New("markMessageSent must be called in pair with NextSNToSend!")
	}
	w.unAckedList[sn] = m
	w.minUnsentSN++
	return nil
}

func (w *slidingWindowSender) ackMessage(sn int) {
	delete(w.unAckedList, sn)
	if sn == w.l {
		w.l++
	}
}

func (w *slidingWindowSender) cackMessage(sn int) {
	for i := w.l; i <= sn; i++ {
		delete(w.unAckedList, i)
	}
	w.l = sn + 1
}

func (w *slidingWindowSender) empty() bool {
	return len(w.unAckedList) == 0
}
