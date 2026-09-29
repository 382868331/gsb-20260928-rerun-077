package raft

import "sort"

// Tick advances a node by one logical tick and returns any messages it
// wants to send. Nodes have no access to a clock other than the tick
// counter handed to them, and no access to other nodes' state.
func (n *Node) Tick(tick, clusterSize int) []Message {
	switch n.Role {
	case Leader:
		if tick-n.lastHB >= HeartbeatTicks {
			n.lastHB = tick
			return n.broadcastAppend(clusterSize)
		}
	case Follower, Candidate:
		if tick-n.lastHB >= n.electionTO {
			return n.startElection(tick, clusterSize)
		}
	}
	return nil
}

func (n *Node) startElection(tick, clusterSize int) []Message {
	n.CurrentTerm++
	n.Role = Candidate
	n.LeaderID = -1
	n.VotedFor = n.ID
	n.lastHB = tick
	n.resetElectionTO()
	n.granted = map[int]bool{n.ID: true}

	msgs := []Message{
		{
			Type:         "voteReq",
			From:         n.ID,
			To:           n.ID,
			Term:         n.CurrentTerm,
			LastLogIndex: n.lastLogIndex(),
			LastLogTerm:  n.lastLogTerm(),
		},
	}
	req := Message{
		Type:         "voteReq",
		From:         n.ID,
		Term:         n.CurrentTerm,
		LastLogIndex: n.lastLogIndex(),
		LastLogTerm:  n.lastLogTerm(),
	}
	for peer := 0; peer < clusterSize; peer++ {
		if peer == n.ID {
			continue
		}
		m := req
		m.To = peer
		msgs = append(msgs, m)
	}
	return msgs
}

// Step processes one delivered message. It is the sole entry point for
// information from the outside world.
func (n *Node) Step(m Message, tick, clusterSize int) []Message {
	if m.Term > n.CurrentTerm {
		n.becomeFollower(m.Term, -1)
	}

	switch m.Type {
	case "voteReq":
		return n.handleVoteReq(m)
	case "voteResp":
		return n.handleVoteResp(m, tick, clusterSize)
	case "appReq":
		return n.handleAppendReq(m, tick)
	case "appResp":
		return n.handleAppendResp(m, tick, clusterSize)
	case "client":
		return n.handleClient(m)
	}
	return nil
}

func (n *Node) handleVoteReq(m Message) []Message {
	resp := Message{Type: "voteResp", From: n.ID, To: m.From, Term: n.CurrentTerm}

	uptoDate := m.LastLogTerm > n.lastLogTerm() ||
		(m.LastLogTerm == n.lastLogTerm() && m.LastLogIndex >= n.lastLogIndex())

	canGrant := m.Term == n.CurrentTerm &&
		(n.VotedFor == -1 || n.VotedFor == m.From) && uptoDate
	if canGrant {
		n.VotedFor = m.From
		n.LeaderID = -1
		n.lastHB = m.AtTick
		resp.Granted = true
	} else {
		// Refresh liveness on contact from a current-term peer so
		// partitions don't make followers spin uselessly.
		if m.Term >= n.CurrentTerm {
			n.lastHB = m.AtTick
		}
	}
	return []Message{resp}
}

func (n *Node) handleVoteResp(m Message, tick, clusterSize int) []Message {
	if n.Role != Candidate || m.Term != n.CurrentTerm || !m.Granted {
		return nil
	}
	votes := 1 // self
	n.granted[m.From] = true
	votes = len(n.granted)
	if votes*2 > clusterSize {
		return n.becomeLeader(tick, clusterSize)
	}
	return nil
}

func (n *Node) becomeLeader(tick, clusterSize int) []Message {
	n.Role = Leader
	n.LeaderID = n.ID
	n.lastHB = tick
	n.NextIndex = make([]int, clusterSize)
	n.MatchIndex = make([]int, clusterSize)
	for peer := range n.NextIndex {
		n.NextIndex[peer] = n.lastLogIndex() + 1
		n.MatchIndex[peer] = 0
	}
	n.MatchIndex[n.ID] = n.lastLogIndex()
	return n.broadcastAppend(clusterSize)
}

func (n *Node) broadcastAppend(clusterSize int) []Message {
	var msgs []Message
	for peer := 0; peer < clusterSize; peer++ {
		if peer == n.ID {
			continue
		}
		next := n.NextIndex[peer]
		prev := next - 1
		var send []Entry
		for i := next; i <= n.lastLogIndex(); i++ {
			send = append(send, n.Log[i])
		}
		msgs = append(msgs, Message{
			Type:         "appReq",
			From:         n.ID,
			To:           peer,
			Term:         n.CurrentTerm,
			PrevLogIndex: prev,
			PrevLogTerm:  n.termAt(prev),
			Entries:      send,
			LeaderCommit: n.CommitIdx,
		})
	}
	return msgs
}

func (n *Node) handleAppendReq(m Message, tick int) []Message {
	resp := Message{Type: "appResp", From: n.ID, To: m.From, Term: n.CurrentTerm}

	if m.Term < n.CurrentTerm {
		return []Message{resp}
	}

	n.becomeFollower(m.Term, m.From)
	n.lastHB = tick

	if n.termAt(m.PrevLogIndex) != m.PrevLogTerm {
		resp.MatchIndex = n.lastLogIndex()
		return []Message{resp}
	}

	// Append any new entries not already in the log, deleting conflicts.
	for _, e := range m.Entries {
		if e.Index <= n.lastLogIndex() {
			if n.termAt(e.Index) == e.Term {
				continue
			}
			n.Log = n.Log[:e.Index]
		}
		n.Log = append(n.Log, e)
	}

	resp.Success = true
	resp.MatchIndex = n.lastLogIndex()

	if m.LeaderCommit > n.CommitIdx {
		if m.LeaderCommit < n.lastLogIndex() {
			n.CommitIdx = m.LeaderCommit
		} else {
			n.CommitIdx = n.lastLogIndex()
		}
	}
	return []Message{resp}
}

func (n *Node) handleAppendResp(m Message, tick, clusterSize int) []Message {
	if n.Role != Leader || m.Term != n.CurrentTerm {
		return nil
	}

	if m.Success {
		if m.MatchIndex > n.MatchIndex[m.From] {
			n.MatchIndex[m.From] = m.MatchIndex
		}
		next := m.MatchIndex + 1
		if next > n.NextIndex[m.From] {
			n.NextIndex[m.From] = next
		}
		n.advanceCommit(clusterSize)
		return n.flushPending()
	}

	// Back up NextIndex and retry the preceding entry on the next
	// heartbeat (keeps message handling simple and deterministic).
	if n.NextIndex[m.From] > 1 {
		n.NextIndex[m.From]--
	} else {
		n.NextIndex[m.From] = 1
	}
	n.lastHB = tick
	return nil
}

// advanceCommit implements Raft's majority-on-current-term commit rule.
func (n *Node) advanceCommit(clusterSize int) {
	replicated := make([]int, 0, clusterSize)
	for id := 0; id < clusterSize; id++ {
		replicated = append(replicated, n.MatchIndex[id])
	}
	sort.Ints(replicated)
	majorityIdx := replicated[clusterSize/2] // median = majority watermark

	for idx := n.CommitIdx + 1; idx <= majorityIdx; idx++ {
		if n.termAt(idx) == n.CurrentTerm {
			n.CommitIdx = idx
		}
	}
}

func (n *Node) handleClient(m Message) []Message {
	if n.Role != Leader {
		return []Message{{
			Type: "clientAck", From: n.ID, To: m.From, Term: n.CurrentTerm,
			ClientID: m.ClientID, OK: false, Reason: "not-leader",
		}}
	}
	e := Entry{Index: n.lastLogIndex() + 1, Term: n.CurrentTerm, Data: m.Data}
	n.Log = append(n.Log, e)
	n.MatchIndex[n.ID] = e.Index
	n.pending[e.Index] = m.ClientID

	// Try immediate commit (single-node cluster) and replicate out.
	acks := n.flushPending()
	acks = append(acks, n.broadcastAppend(len(n.NextIndex))...)
	return acks
}

// flushPending answers client proposals whose entries are now committed.
func (n *Node) flushPending() []Message {
	var acks []Message
	for idx, clientID := range n.pending {
		if idx <= n.CommitIdx {
			acks = append(acks, Message{
				Type: "clientAck", From: n.ID, To: -1,
				Term: n.CurrentTerm, ClientID: clientID, OK: true,
				MatchIndex: idx,
			})
			delete(n.pending, idx)
		}
	}
	return acks
}
