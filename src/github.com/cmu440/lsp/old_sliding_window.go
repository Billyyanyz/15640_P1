package lsp

import "errors"

type oldSlidingWindowSender struct {
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
func newOldSlidingWindowSender(sn int, windowSize int, maxUnackedMsg int) oldSlidingWindowSender {
	s := oldSlidingWindowSender{
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

func (w *oldSlidingWindowSender) nextMsgToSend() (int, *Message) {
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

func (w *oldSlidingWindowSender) getSeqNum() int {
	res := w.nextSN
	w.nextSN++
	return res
}

func (w *oldSlidingWindowSender) backupUnsentMsg(m *Message) {
	w.unsentData[m.SeqNum] = m
}

func (w *oldSlidingWindowSender) markMessageSent(m *Message) error {
	sn := m.SeqNum
	if sn != w.minUnsentSN {
		return errors.New("markMessageSent must be called in pair with NextSNToSend")
	}
	w.unAckedList[sn] = m
	w.minUnsentSN++
	return nil
}

func (w *oldSlidingWindowSender) ackMessage(sn int) {
	if _, ok := w.unAckedList[sn]; ok {
		delete(w.unAckedList, sn)
		if sn == w.l {
			w.l++
		}
	}
}

func (w *oldSlidingWindowSender) cackMessage(sn int) {
	for i := w.l; i <= sn; i++ {
		if _, ok := w.unAckedList[i]; ok {
			delete(w.unAckedList, i)
		}
	}
	if w.l <= sn {
		w.l = sn + 1
	}
}

func (w *oldSlidingWindowSender) empty() bool {
	return len(w.unAckedList) == 0
}
