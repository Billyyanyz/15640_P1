package lsp

type messageWithBackoff struct {
	message        *Message
	lastSentEpoch  int
	currentBackoff int
}

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
	unAckedList   map[int]*messageWithBackoff // [l, minUnsentSN)
	unsentData    map[int]*Message            // [minUnsentSN, nextSN)
	maxUnackedMsg int
	maxBackoff    int
}

// Must be called AFTER the handshake is complete
func newSlidingWindowSender(sn int, windowSize int, maxUnackedMsg int, maxBackoff int) slidingWindowSender {
	s := slidingWindowSender{
		l:             sn + 1,
		minUnsentSN:   sn + 1,
		nextSN:        sn + 1,
		size:          windowSize,
		unAckedList:   make(map[int]*messageWithBackoff),
		unsentData:    make(map[int]*Message),
		maxUnackedMsg: maxUnackedMsg,
		maxBackoff:    maxBackoff,
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

func (w *slidingWindowSender) markNextMessageSent(m *Message, e int) {
	delete(w.unsentData, w.minUnsentSN)
	w.unAckedList[m.SeqNum] = &messageWithBackoff{m, e, 0}
	w.minUnsentSN++
}

func (w *slidingWindowSender) ackMessage(sn int) {
	if _, ok := w.unAckedList[sn]; ok {
		delete(w.unAckedList, sn)
		if sn == w.l {
			var i int = sn
			for ; i < w.minUnsentSN; i++ {
				_, found := w.unAckedList[i]
				if found {
					break
				} else {
					w.l++
				}
			}
		}
	}
}

func (w *slidingWindowSender) cackMessage(sn int) {
	for i := w.l; i <= sn; i++ {
		if _, ok := w.unAckedList[i]; ok {
			delete(w.unAckedList, i)
		}
	}

	if w.l <= sn {
		w.l = sn
		var i int = sn
		for ; i < w.minUnsentSN; i++ {
			_, found := w.unAckedList[i]
			if found {
				break
			} else {
				w.l++
			}
		}
	}
}

func (w *slidingWindowSender) empty() bool {
	return (len(w.unAckedList) == 0 && len(w.unsentData) == 0)
}

func (w *slidingWindowSender) resendMessageList(epochCnt int) []*Message {
	res := make([]*Message, 0)
	for _, mwb := range w.unAckedList {
		if epochCnt >= mwb.lastSentEpoch+mwb.currentBackoff+1 {
			res = append(res, mwb.message)
			mwb.lastSentEpoch = epochCnt
			if mwb.currentBackoff == 0 {
				mwb.currentBackoff = 1
			} else {
				mwb.currentBackoff = mwb.currentBackoff * 2
			}
			if mwb.currentBackoff > w.maxBackoff {
				mwb.currentBackoff = w.maxBackoff
			}
		}
	}
	return res
}
