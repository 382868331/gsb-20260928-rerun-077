package raft

import "fmt"

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

const (
	HeartbeatInterval = int64(50)
	ElectionMin       = int64(150)
	ElectionJitter    = int64(150)
)

type Node struct {
	ID               int          `json:"id"`
	Role             Role         `json:"role"`
	Term             int          `json:"term"`
	VotedFor         *int         `json:"votedFor"`
	Log              *Log         `json:"-"`
	CommitIndex      int          `json:"commitIndex"`
	ElectionDeadline int64        `json:"-"`
	HeartbeatDue     int64        `json:"-"`
	VotesGranted     map[int]bool `json:"-"`
	NextIndex        map[int]int  `json:"-"`
	MatchIndex       map[int]int  `json:"-"`
	Peers            []int        `json:"-"`
}

func NewNode(id int, peers []int) *Node {
	return &Node{ID: id, Role: Follower, Log: NewLog(), Peers: peers}
}

func (n *Node) resetElection(rng func(int64) int64, now int64) int64 {
	n.ElectionDeadline = now + ElectionMin + rng(ElectionJitter)
	return n.ElectionDeadline
}

func (n *Node) becomeFollower(term int, rng func(int64) int64, now int64) bool {
	changed := n.Role != Follower || n.Term != term
	n.Role = Follower
	n.Term = term
	n.VotedFor = nil
	n.VotesGranted = nil
	n.NextIndex = nil
	n.MatchIndex = nil
	n.resetElection(rng, now)
	return changed
}

func (n *Node) becomeCandidate(rng func(int64) int64, now int64, emit func(Message)) {
	n.Role = Candidate
	n.Term++
	self := n.ID
	n.VotedFor = &self
	n.VotesGranted = map[int]bool{n.ID: true}
	n.NextIndex = nil
	n.MatchIndex = nil
	n.resetElection(rng, now)
	lastTerm := n.Log.lastTerm()
	for _, peer := range n.Peers {
		emit(Message{Type: MsgRequestVote, From: n.ID, To: peer, Term: n.Term,
			LastLogIdx: n.Log.lastIndex(), LastLogTerm: lastTerm})
	}
}

func (n *Node) becomeLeader(now int64, emit func(Message)) {
	n.Role = Leader
	n.NextIndex = map[int]int{}
	n.MatchIndex = map[int]int{}
	for _, peer := range n.Peers {
		n.NextIndex[peer] = n.Log.lastIndex() + 1
		n.MatchIndex[peer] = 0
	}
	n.HeartbeatDue = now
	n.broadcastAppend(emit)
}

func (n *Node) logUpToDate(lastIndex, lastTerm int) bool {
	myTerm := n.Log.lastTerm()
	myIndex := n.Log.lastIndex()
	return lastTerm > myTerm || (lastTerm == myTerm && lastIndex >= myIndex)
}

func (n *Node) Handle(m Message, rng func(int64) int64, now int64, emit func(Message)) []string {
	notes := []string{}
	if m.Term > n.Term && m.Type != MsgClient {
		if n.becomeFollower(m.Term, rng, now) {
			notes = append(notes, fmt.Sprintf("term-%d-step-down", m.Term))
		}
	}
	switch m.Type {
	case MsgRequestVote:
		grant := m.Term == n.Term && (n.VotedFor == nil || *n.VotedFor == m.From) && n.logUpToDate(m.LastLogIdx, m.LastLogTerm)
		if grant {
			n.VotedFor = &m.From
			n.ElectionDeadline = now + ElectionMin + rng(ElectionJitter)
		}
		emit(Message{Type: MsgVoteResponse, From: n.ID, To: m.From, Term: n.Term, Granted: grant})
	case MsgVoteResponse:
		if n.Role == Candidate && m.Term == n.Term && m.Granted {
			n.VotesGranted[m.From] = true
			if len(n.VotesGranted)*2 > len(n.Peers)+1 {
				n.becomeLeader(now, emit)
				notes = append(notes, "elected")
			}
		}
	case MsgAppend:
		n.handleAppend(m, rng, now, emit, &notes)
	case MsgAppendResp:
		n.handleAppendResp(m, emit, &notes)
	case MsgClient:
		if n.Role != Leader {
			notes = append(notes, "rejected-not-leader")
			return notes
		}
		e := n.Log.append(n.Term, m.Data)
		n.MatchIndex[n.ID] = e.Index
		notes = append(notes, fmt.Sprintf("appended-%d", e.Index))
		n.broadcastAppend(emit)
	}
	return notes
}

func (n *Node) handleAppend(m Message, rng func(int64) int64, now int64, emit func(Message), notes *[]string) {
	if m.Term < n.Term {
		emit(Message{Type: MsgAppendResp, From: n.ID, To: m.From, Term: n.Term, Success: false,
			ConflictIdx: n.Log.lastIndex() + 1})
		return
	}
	if n.Role != Follower {
		n.becomeFollower(m.Term, rng, now)
	}
	n.ElectionDeadline = now + ElectionMin + rng(ElectionJitter)
	if m.PrevLogIdx > n.Log.lastIndex() {
		emit(Message{Type: MsgAppendResp, From: n.ID, To: m.From, Term: n.Term, Success: false,
			ConflictIdx: n.Log.lastIndex() + 1})
		return
	}
	term, ok := n.Log.termAt(m.PrevLogIdx)
	if !ok || term != m.PrevLogTerm {
		conflict := m.PrevLogIdx
		for conflict > 0 {
			t, exists := n.Log.termAt(conflict)
			if !exists || t != m.PrevLogTerm {
				break
			}
			conflict--
		}
		emit(Message{Type: MsgAppendResp, From: n.ID, To: m.From, Term: n.Term, Success: false, ConflictIdx: conflict + 1})
		return
	}
	for _, entry := range m.Entries {
		if existingTerm, exists := n.Log.termAt(entry.Index); exists {
			if existingTerm != entry.Term {
				n.Log.truncate(entry.Index - 1)
				n.Log.entries = append(n.Log.entries, entry)
			}
			continue
		}
		if entry.Index == n.Log.lastIndex()+1 {
			n.Log.entries = append(n.Log.entries, entry)
		}
	}
	if m.LeaderCommit > n.CommitIndex {
		old := n.CommitIndex
		n.CommitIndex = min(m.LeaderCommit, n.Log.lastIndex())
		if n.CommitIndex > old {
			*notes = append(*notes, fmt.Sprintf("committed-%d", n.CommitIndex))
		}
	}
	emit(Message{Type: MsgAppendResp, From: n.ID, To: m.From, Term: n.Term, Success: true,
		ConflictIdx: n.Log.lastIndex()})
}

func (n *Node) handleAppendResp(m Message, emit func(Message), notes *[]string) {
	if n.Role != Leader || m.Term != n.Term {
		return
	}
	if m.Success {
		n.MatchIndex[m.From] = max(n.MatchIndex[m.From], m.ConflictIdx)
		n.NextIndex[m.From] = n.MatchIndex[m.From] + 1
		n.advanceCommit(notes)
		return
	}
	next := m.ConflictIdx
	if next <= 0 {
		next = 1
	}
	if next >= n.NextIndex[m.From] {
		next = n.NextIndex[m.From] - 1
	}
	n.NextIndex[m.From] = max(1, next)
	n.sendAppend(m.From, emit)
}

func (n *Node) advanceCommit(notes *[]string) {
	for candidate := n.Log.lastIndex(); candidate > n.CommitIndex; candidate-- {
		term, _ := n.Log.termAt(candidate)
		if term != n.Term {
			continue
		}
		votes := 1
		for _, peer := range n.Peers {
			if n.MatchIndex[peer] >= candidate {
				votes++
			}
		}
		if votes*2 > len(n.Peers)+1 {
			n.CommitIndex = candidate
			*notes = append(*notes, fmt.Sprintf("committed-%d", candidate))
			return
		}
	}
}

func (n *Node) TimeoutFired(now int64) bool {
	if n.Role == Leader {
		return now >= n.HeartbeatDue
	}
	return now >= n.ElectionDeadline
}

func (n *Node) HandleTimeout(rng func(int64) int64, now int64, emit func(Message)) string {
	if n.Role == Leader {
		n.HeartbeatDue = now + HeartbeatInterval
		n.broadcastAppend(emit)
		return "heartbeat"
	}
	n.becomeCandidate(rng, now, emit)
	return fmt.Sprintf("election-term-%d", n.Term)
}

func (n *Node) broadcastAppend(emit func(Message)) {
	for _, peer := range n.Peers {
		n.sendAppend(peer, emit)
	}
}

func (n *Node) sendAppend(peer int, emit func(Message)) {
	prev := n.NextIndex[peer] - 1
	prevTerm, _ := n.Log.termAt(prev)
	entries := n.Log.entriesAfter(prev)
	emit(Message{Type: MsgAppend, From: n.ID, To: peer, Term: n.Term,
		PrevLogIdx: prev, PrevLogTerm: prevTerm, Entries: entries,
		LeaderCommit: n.CommitIndex})
}
