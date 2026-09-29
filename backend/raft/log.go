package raft

type Entry struct {
	Index int    `json:"index"`
	Term  int    `json:"term"`
	Data  string `json:"data"`
}

type Log struct {
	entries []Entry
}

func NewLog() *Log {
	return &Log{entries: []Entry{{Index: 0, Term: 0, Data: ""}}}
}

func (l *Log) lastIndex() int { return l.entries[len(l.entries)-1].Index }
func (l *Log) lastTerm() int  { return l.entries[len(l.entries)-1].Term }

func (l *Log) termAt(index int) (int, bool) {
	if index < 0 || index >= len(l.entries) {
		return 0, false
	}
	return l.entries[index].Term, true
}

func (l *Log) append(term int, data string) Entry {
	e := Entry{Index: l.lastIndex() + 1, Term: term, Data: data}
	l.entries = append(l.entries, e)
	return e
}

func (l *Log) truncate(index int) {
	if index < 0 {
		index = 0
	}
	if index < len(l.entries) {
		l.entries = append([]Entry(nil), l.entries[:index+1]...)
	}
}

func (l *Log) entriesAfter(index int) []Entry {
	if index < 0 {
		index = 0
	}
	if index >= len(l.entries) {
		return nil
	}
	out := make([]Entry, len(l.entries)-index-1)
	copy(out, l.entries[index+1:])
	return out
}

func (l *Log) snapshot() []Entry {
	out := make([]Entry, len(l.entries)-1)
	copy(out, l.entries[1:])
	return out
}
