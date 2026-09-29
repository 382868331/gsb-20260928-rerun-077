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
