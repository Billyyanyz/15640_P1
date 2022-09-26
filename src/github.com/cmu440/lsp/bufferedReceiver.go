package lsp

type bufferedReceiver struct {
	l    int
	data map[int]*Message
}

func newBufferedReceiver(sn int) bufferedReceiver {
	s := bufferedReceiver{
		l:    sn + 1,
		data: make(map[int]*Message),
	}
	return s
}

func (w *bufferedReceiver) recvMsg(m *Message) {
	w.data[m.SeqNum] = m
}

func (w *bufferedReceiver) readyToRead() bool {
	_, ok := w.data[w.l]
	return ok
}

func (w *bufferedReceiver) deliverToRead() (m *Message) {
	m = w.data[w.l]
	delete(w.data, w.l)
	w.l++
	return m
}
