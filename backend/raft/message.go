package raft

// Message is the only way nodes can learn about each other. Every
// RequestVote / AppendEntries exchange is represented by one pair of
// messages travelling through the simulated network.
type Message struct {
	Type   string `json:"type"` // voteReq, voteResp, appReq, appResp, clientAck
	From   int    `json:"from"`
	To     int    `json:"to"`
	Term   int    `json:"term"`
	AtTick int    `json:"atTick"`

	// RequestVote
	LastLogIndex int  `json:"lastLogIndex,omitempty"`
	LastLogTerm  int  `json:"lastLogTerm,omitempty"`
	Granted      bool `json:"granted,omitempty"`

	// AppendEntries
	PrevLogIndex int     `json:"prevLogIndex,omitempty"`
	PrevLogTerm  int     `json:"prevLogTerm,omitempty"`
	Entries      []Entry `json:"entries,omitempty"`
	LeaderCommit int     `json:"leaderCommit,omitempty"`
	Success      bool    `json:"success,omitempty"`
	MatchIndex   int     `json:"matchIndex,omitempty"`

	// Client proposals travel as messages too, so a partitioned would-be
	// leader can only answer once a quorum has actually acked.
	ClientID string `json:"clientId,omitempty"`
	OK       bool   `json:"ok,omitempty"`
	Reason   string `json:"reason,omitempty"`
	Data     string `json:"data,omitempty"`
}
