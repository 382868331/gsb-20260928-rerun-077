package raft

import "strconv"

func clientID(seq int) string { return "c" + strconv.Itoa(seq) }

func itoa(i int) string { return strconv.Itoa(i) }

func (e *Engine) handleClientAck(m Message) {
	target := -1
	if t, ok := e.clients[m.ClientID]; ok {
		target = t
	}
	if m.OK {
		e.emit(Event{
			Kind: "commit", A: m.From, B: target, Term: m.Term,
			Text: "[OK] committed index " + itoa(m.MatchIndex) + " term " + itoa(m.Term),
		})
	} else {
		reason := m.Reason
		if reason == "" {
			reason = "rejected"
		}
		e.emit(Event{
			Kind: "reject", A: m.From, B: target, Term: m.Term,
			Text: "[FAIL] proposal to node" + itoa(target) + " failed (" + reason + "): no quorum",
		})
	}
}

// Snapshot returns the full UI state. It is read-only: the UI never gets
// a magic channel into node internals for decisions; nodes decide only
// from delivered messages.
func (e *Engine) Snapshot() Snapshot {
	snap := Snapshot{
		Tick:   e.tick,
		Nodes:  make([]NodeSnapshot, e.Size),
		Links:  map[string]LinkState{},
		Leader: -1,
		Quorum: e.Size/2 + 1,
	}
	for i, n := range e.Nodes {
		entries := make([]Entry, 0, len(n.Log)-1)
		entries = append(entries, n.Log[1:]...)
		snap.Nodes[i] = NodeSnapshot{
			ID:        n.ID,
			Role:      n.Role,
			Term:      n.CurrentTerm,
			VotedFor:  n.VotedFor,
			LeaderID:  n.LeaderID,
			CommitIdx: n.CommitIdx,
			Log:       LogView{Entries: entries, Commit: n.CommitIdx},
		}
	}
	for k, l := range e.links {
		snap.Links[k] = *l
	}
	snap.Leader = e.currentLeader()
	return snap
}

// currentLeader reports a node only if it currently leads the term with a
// live quorum of links. A leader isolated from the majority is reported
// as unknown even though it still calls itself leader internally.
func (e *Engine) currentLeader() int {
	for _, n := range e.Nodes {
		if n.Role != Leader {
			continue
		}
		reachable := 1
		for peer := 0; peer < e.Size; peer++ {
			if peer != n.ID && e.linkUp(n.ID, peer) {
				reachable++
			}
		}
		if reachable*2 > e.Size {
			return n.ID
		}
	}
	return -1
}

func (e *Engine) Events() []Event {
	out := make([]Event, len(e.events))
	copy(out, e.events)
	return out
}

func (e *Engine) Actions() []Action {
	out := make([]Action, len(e.actions))
	copy(out, e.actions)
	return out
}

func (e *Engine) TickValue() int { return e.tick }
