package lsp

// change to sending window!!!

type slidingWindowSender struct {
	l             int
	currentSN     int
	size          int
	data          map[int]*Message
	maxUnackedMsg int
}

func newSlidingWindowSender(sn int, windowSize int, maxUnackedMsg int) slidingWindowSender {
	s := slidingWindowSender{
		l:             sn,
		currentSN:     sn,
		size:          windowSize,
		data:          make(map[int]*Message),
		maxUnackedMsg: maxUnackedMsg,
	}
	return s
}

func (w *slidingWindowSender) getSeriesNum() int {
	if len(w.data) >= w.maxUnackedMsg {
		return -1
	}
	if w.currentSN >= w.l+w.size {
		return -1
	}
	w.currentSN++
	return w.currentSN - 1
}

func (w *slidingWindowSender) backupMsg(m *Message) {
	w.data[m.SeqNum] = m
}

func (w *slidingWindowSender) ackMessage(sn int) {
	delete(w.data, sn)
	if sn == w.l {
		w.l++
	}
}

func (w *slidingWindowSender) cackMessage(sn int) {
	for i := w.l; i <= sn; i++ {
		delete(w.data, i)
	}
	w.l = sn + 1
}
