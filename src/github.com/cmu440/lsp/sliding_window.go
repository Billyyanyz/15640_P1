package lsp

type slidingWindowReceiver struct {
	l           int
	size        int
	data        map[int]*Message
	maxUnackMsg int
}

func newSlidingWindowReceiver(sn int, windowSize int, maxUnackMsg int) slidingWindowReceiver {
	s := slidingWindowReceiver{
		l:           sn,
		size:        windowSize,
		data:        make(map[int]*Message),
		maxUnackMsg: maxUnackMsg,
	}
	return s
}

func (w *slidingWindowReceiver) outsideWindow(m *Message) bool {
	return m.SeqNum >= w.l+w.size
}

func (w *slidingWindowReceiver) recvMsg(m *Message) {
	w.data[m.SeqNum] = m
	for i := w.l; i < w.l+w.size; i++ {
		if _, ok := w.data[i]; !ok {
			w.l = i
			break
		}
	}
}
