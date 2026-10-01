package raft

type MessageType string

const (
	MsgRequestVote  MessageType = "RequestVote"
	MsgVoteResponse MessageType = "VoteResponse"
	MsgAppend       MessageType = "AppendEntries"
	MsgAppendResp   MessageType = "AppendEntriesResponse"
	MsgClient       MessageType = "ClientCommand"
)

type Message struct {
	ID           int         `json:"id"`
	Type         MessageType `json:"type"`
	From         int         `json:"from"`
	To           int         `json:"to"`
	Term         int         `json:"term"`
	Data         string      `json:"data,omitempty"`
	LastLogIdx   int         `json:"lastLogIndex,omitempty"`
	LastLogTerm  int         `json:"lastLogTerm,omitempty"`
	Granted      bool        `json:"granted,omitempty"`
	PrevLogIdx   int         `json:"prevLogIndex,omitempty"`
	PrevLogTerm  int         `json:"prevLogTerm,omitempty"`
	Entries      []Entry     `json:"entries,omitempty"`
	LeaderCommit int         `json:"leaderCommit,omitempty"`
	Success      bool        `json:"success,omitempty"`
	ConflictIdx  int         `json:"conflictIndex,omitempty"`
	SendAt       int64       `json:"sendAt"`
	DeliverAt    int64       `json:"deliverAt"`
}

// Selector identifies queued messages independently of their delivery ID,
// so pause points stay stable across forks and replays.
type Selector struct {
	Type MessageType `json:"type,omitempty"`
	From int         `json:"from,omitempty"`
	To   int         `json:"to,omitempty"`
	Term int         `json:"term,omitempty"`
	Data string      `json:"data,omitempty"`
}

func (sel Selector) Matches(m Message) bool {
	if sel.Type != "" && sel.Type != m.Type {
		return false
	}
	if sel.From != 0 && sel.From != m.From {
		return false
	}
	if sel.To != 0 && sel.To != m.To {
		return false
	}
	if sel.Term != 0 && sel.Term != m.Term {
		return false
	}
	if sel.Data != "" && sel.Data != m.Data {
		return false
	}
	return true
}
