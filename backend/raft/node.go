package raft

import (
	"math/rand"
)

const (
	HeartbeatTicks = 5
	MinElectionTO  = 15
	MaxElectionTO  = 30
)

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

// Entry is one replicated log record.
type Entry struct {
	Index int    `json:"index"`
	Term  int    `json:"term"`
	Data  string `json:"data"`
}

type Node struct {
	ID int

	// Persistent state (on stable storage in real Raft; kept in a struct
	// that the engine snapshots so restart/replay behaves the same).
	CurrentTerm int
	VotedFor    int     // -1 when no vote cast this term
	Log         []Entry // index 0 is a sentinel

	// Volatile state.
	Role        Role
	LeaderID    int // -1 if unknown
	CommitIdx   int
	LastApplied int

	// Leader-only volatile state.
	NextIndex  []int
	MatchIndex []int

	// Election timeout / heartbeat scheduling.
	electionTO int
	lastHB     int
	rng        *rand.Rand

	// In-flight client proposals keyed by log index.
	pending map[int]string

	// Voters granted in the current election.
	granted map[int]bool
}

func newNode(id, clusterSize, seed int) *Node {
	n := &Node{
		ID:       id,
		VotedFor: -1,
		LeaderID: -1,
		Role:     Follower,
		Log:      []Entry{{Index: 0, Term: 0, Data: ""}},
		pending:  map[int]string{},
		granted:  map[int]bool{},
		rng:      rand.New(rand.NewSource(int64(seed)*1009 + int64(id))),
	}
	n.resetElectionTO()
	return n
}

func (n *Node) resetElectionTO() {
	n.electionTO = MinElectionTO + n.rng.Intn(MaxElectionTO-MinElectionTO+1)
}

func (n *Node) lastLogIndex() int {
	return n.Log[len(n.Log)-1].Index
}

func (n *Node) lastLogTerm() int {
	return n.Log[len(n.Log)-1].Term
}

// termAt returns the term of the entry at index idx (sentinel term 0).
func (n *Node) termAt(idx int) int {
	if idx <= 0 {
		return 0
	}
	for i := len(n.Log) - 1; i >= 0; i-- {
		if n.Log[i].Index == idx {
			return n.Log[i].Term
		}
	}
	return -1
}

func (n *Node) becomeFollower(term, leaderID int) {
	if term > n.CurrentTerm {
		n.VotedFor = -1
	}
	n.CurrentTerm = term
	n.Role = Follower
	n.LeaderID = leaderID
	n.granted = map[int]bool{}
	n.resetElectionTO()
}

func (n *Node) lastContact() int { return n.lastHB }
